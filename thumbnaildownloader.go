package main

import (
	"context"
	"database/sql"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"fknsrs.biz/p/sorm"
	"github.com/beanstalkd/go-beanstalk"
	"github.com/pkg/errors"
	"github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"
)

type thumbnailDownloader struct {
	m       sync.RWMutex
	db      *sql.DB
	newBS   beanstalkdFactory
	workers map[uuid.UUID]*thumbnailDownloaderWorker
}

func newThumbnailDownloader(db *sql.DB, newBS beanstalkdFactory, workerCount int) (*thumbnailDownloader, error) {
	d := &thumbnailDownloader{
		db:      db,
		newBS:   newBS,
		workers: make(map[uuid.UUID]*thumbnailDownloaderWorker),
	}

	if err := d.setWorkerCount(workerCount); err != nil {
		if err := d.setWorkerCount(0); err != nil {
			return nil, err
		}

		return nil, err
	}

	return d, nil
}

func (d *thumbnailDownloader) setWorkerCount(workerCount int) error {
	switch {
	case len(d.workers) < workerCount:
		for i := len(d.workers); i < workerCount; i++ {
			id := uuid.Must(uuid.NewV4())

			logrus.WithField("worker", id).Info("starting thumbnail downloader worker")

			worker, err := newThumbnailDownloaderWorker(id, d.db, d.newBS)
			if err != nil {
				return err
			}

			go func() {
				defer func() {
					if err := worker.shutdown(); err != nil {
						logrus.WithField("worker", id).WithError(err).Warn("error shutting down worker")
					}

					d.m.Lock()
					delete(d.workers, id)
					d.m.Unlock()
				}()

				if err := worker.run(); err != nil {
					logrus.WithField("worker", id).WithError(err).Warn("error running worker")
				}
			}()

			d.m.Lock()
			d.workers[id] = worker
			d.m.Unlock()
		}
	case len(d.workers) > workerCount:
		for len(d.workers) > workerCount {
			for id, worker := range d.workers {
				logrus.WithField("worker", id).Info("stopping thumbnail downloader worker")

				if err := worker.shutdown(); err != nil {
					return err
				}

				logrus.WithField("worker", id).Info("stopped thumbnail downloader worker")

				break
			}
		}
	}

	return nil
}

type thumbnailDownloaderWorker struct {
	id      uuid.UUID
	db      *sql.DB
	bs      *beanstalk.Conn
	closing bool
}

func newThumbnailDownloaderWorker(id uuid.UUID, db *sql.DB, newBS beanstalkdFactory) (*thumbnailDownloaderWorker, error) {
	bs, err := newBS()
	if err != nil {
		return nil, err
	}

	return &thumbnailDownloaderWorker{id: id, db: db, bs: bs}, nil
}

func (w *thumbnailDownloaderWorker) shutdown() error {
	if w.closing {
		return nil
	}

	w.closing = true

	if err := w.bs.Close(); err != nil {
		return err
	}

	return nil
}

func (w *thumbnailDownloaderWorker) run() error {
	l := logrus.WithFields(logrus.Fields{"system": "thumbnail-downloader", "worker": w.id})

	tube := beanstalk.NewTubeSet(w.bs, thumbnailDownloaderQueue)

	for {
		l.Debug("reserving job")

		jobID, videoID, err := tube.Reserve(time.Hour)
		if err != nil {
			if berr, ok := err.(beanstalk.ConnError); ok {
				if berr.Err == beanstalk.ErrTimeout {
					continue
				}

				if nerr, ok := berr.Err.(*net.OpError); ok {
					if strings.Contains(nerr.Err.Error(), "use of closed network connection") && w.closing {
						return nil
					}

					l.WithError(nerr.Err).Error("network error")
				}

				l.WithError(berr.Err).Error("beanstalkd error")
			}

			l.WithError(err).Error("couldn't get job from beanstalkd")

			return err
		}

		l := l.WithFields(logrus.Fields{"job_id": jobID, "video": string(videoID)})

		stats, err := w.bs.StatsJob(jobID)
		if err != nil {
			if berr, ok := err.(beanstalk.ConnError); ok {
				if nerr, ok := berr.Err.(*net.OpError); ok {
					if strings.Contains(nerr.Err.Error(), "use of closed network connection") && w.closing {
						return nil
					}

					l.WithError(nerr.Err).Error("network error")
				}

				l.WithError(berr.Err).Error("beanstalkd error")
			}

			l.WithError(err).Error("couldn't get job stats from beanstalkd")

			return err
		}

		reserveCount, err := strconv.ParseInt(stats["reserves"], 10, 64)
		if err != nil {
			l.WithError(err).Error("couldn't parse reserves count from job stats")

			if err := w.bs.Release(jobID, 1, time.Second*10); err != nil {
				l.WithError(err).Error("couldn't release job back to beanstalkd")
			}

			continue
		}

		l = l.WithField("reserve_count", reserveCount)
		l = l.WithField("priority", stats["pri"])
		l = l.WithField("age", stats["age"])

		if reserveCount > 3 {
			l.Warn("reserve count is too high; not running this job again")

			if err := w.bs.Bury(jobID, 1); err != nil {
				l.WithError(err).Error("couldn't bury job in beanstalkd")
			}

			continue
		}

		var v video
		if err := sorm.FindFirstWhere(context.Background(), w.db, &v, "where id = $1", string(videoID)); err != nil {
			l.WithError(err).Error("couldn't get video data from database")

			if err := w.bs.Release(jobID, 1, time.Second*10); err != nil {
				l.WithError(err).Error("couldn't release job back to beanstalkd")
			}

			continue
		}

		if err := w.download(v.ID, v.ThumbnailURL); err != nil {
			l.WithError(err).Error("couldn't download thumbnail")

			if err := w.bs.Release(jobID, 1, time.Second*10); err != nil {
				l.WithError(err).Error("couldn't release job back to beanstalkd")
			}

			continue
		}

		l.Info("downloaded thumbnail")

		if _, err := w.db.Exec("update videos set thumbnail_downloaded_at = $1 where id = $2", time.Now(), v.ID); err != nil {
			l.WithError(err).Error("couldn't save video record")

			if err := w.bs.Release(jobID, 1, time.Second*10); err != nil {
				l.WithError(err).Error("couldn't release job back to beanstalkd")
			}

			continue
		}

		if err := w.bs.Delete(jobID); err != nil {
			l.WithError(err).Error("couldn't remove job from beanstalkd")
		}
	}
}

func (w *thumbnailDownloaderWorker) download(id, url string) error {
	res, err := http.Get(url)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return errors.Errorf("invalid status code; expected 200 but got %d", res.StatusCode)
	}

	fd, err := os.OpenFile("thumbs/"+id+".jpg", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer fd.Close()

	if _, err := io.Copy(fd, res.Body); err != nil {
		return err
	}

	return nil
}
