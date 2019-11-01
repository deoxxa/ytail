package main

import (
	"context"
	"database/sql"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"fknsrs.biz/p/sorm"
	"github.com/beanstalkd/go-beanstalk"
	"github.com/deoxxa/ytdl"
	"github.com/pkg/errors"
	"github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"
)

type videoChecker struct {
	m                 sync.RWMutex
	db                *sql.DB
	newBS             beanstalkdFactory
	validityThreshold time.Duration
	maxAge            time.Duration
	workers           map[uuid.UUID]*videoCheckerWorker
}

func newVideoChecker(db *sql.DB, newBS beanstalkdFactory, validityThreshold, maxAge time.Duration, workerCount int) (*videoChecker, error) {
	c := &videoChecker{
		db:      db,
		newBS:   newBS,
		workers: make(map[uuid.UUID]*videoCheckerWorker),
	}

	c.setValidityThreshold(validityThreshold)
	c.setMaxAge(maxAge)

	if err := c.setWorkerCount(workerCount); err != nil {
		if err := c.setWorkerCount(0); err != nil {
			return nil, err
		}

		return nil, err
	}

	return c, nil
}

func (c *videoChecker) setValidityThreshold(validityThreshold time.Duration) {
	if c.validityThreshold == validityThreshold {
		return
	}

	logrus.WithFields(logrus.Fields{"old_value": c.validityThreshold, "new_value": validityThreshold}).Info("video checker updating expiry threshold")

	c.validityThreshold = validityThreshold

	for _, w := range c.workers {
		w.validityThreshold = validityThreshold
	}
}

func (c *videoChecker) setMaxAge(maxAge time.Duration) {
	if c.maxAge == maxAge {
		return
	}

	logrus.WithFields(logrus.Fields{"old_value": c.maxAge, "new_value": maxAge}).Info("video checker updating expiry threshold")

	c.maxAge = maxAge

	for _, w := range c.workers {
		w.maxAge = maxAge
	}
}

func (c *videoChecker) setWorkerCount(workerCount int) error {
	switch {
	case len(c.workers) < workerCount:
		for i := len(c.workers); i < workerCount; i++ {
			id := uuid.Must(uuid.NewV4())

			logrus.WithField("worker", id).Info("starting video checker worker")

			worker, err := newVideoCheckerWorker(id, c.db, c.newBS, c.validityThreshold, c.maxAge)
			if err != nil {
				return err
			}

			go func() {
				defer func() {
					if err := worker.shutdown(); err != nil {
						logrus.WithField("worker", id).WithError(err).Warn("error shutting down worker")
					}

					c.m.Lock()
					delete(c.workers, id)
					c.m.Unlock()
				}()

				if err := worker.run(); err != nil {
					logrus.WithField("worker", id).WithError(err).Warn("error running worker")
				}
			}()

			c.m.Lock()
			c.workers[id] = worker
			c.m.Unlock()
		}
	case len(c.workers) > workerCount:
		for len(c.workers) > workerCount {
			for id, worker := range c.workers {
				logrus.WithField("worker", id).Info("stopping video checker worker")

				if err := worker.shutdown(); err != nil {
					return err
				}

				logrus.WithField("worker", id).Info("stopped video checker worker")

				break
			}
		}
	}

	return nil
}

type videoCheckerWorker struct {
	id                uuid.UUID
	db                *sql.DB
	bs                *beanstalk.Conn
	validityThreshold time.Duration
	maxAge            time.Duration
	closing           bool
}

func newVideoCheckerWorker(id uuid.UUID, db *sql.DB, newBS beanstalkdFactory, validityThreshold, maxAge time.Duration) (*videoCheckerWorker, error) {
	bs, err := newBS()
	if err != nil {
		return nil, err
	}

	return &videoCheckerWorker{id: id, db: db, bs: bs, validityThreshold: validityThreshold, maxAge: maxAge}, nil
}

func (w *videoCheckerWorker) shutdown() error {
	if w.closing {
		return nil
	}

	w.closing = true

	if err := w.bs.Close(); err != nil {
		return err
	}

	return nil
}

func (w *videoCheckerWorker) run() error {
	l := logrus.WithFields(logrus.Fields{"system": "video-checker", "worker": w.id})

	tube := beanstalk.NewTubeSet(w.bs, videoCheckerQueue)
	ctube := beanstalk.Tube{Conn: w.bs, Name: videoCheckerQueue}
	dtube := beanstalk.Tube{Conn: w.bs, Name: videoDownloaderQueue}

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

			if err := w.bs.Release(jobID, 10, time.Second*10); err != nil {
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

			if err := w.bs.Release(jobID, 10, time.Second*10); err != nil {
				l.WithError(err).Error("couldn't release job back to beanstalkd")
			}

			continue
		}

		if age := time.Now().Sub(v.PublishedAt); age > w.maxAge {
			l.WithField("video_age", age).Warn("video is too old; we won't check it anymore")

			if err := w.bs.Delete(jobID); err != nil {
				l.WithError(err).Error("couldn't delete job from beanstalkd")
			}

			continue
		}

		mp4URL, removalMessage, err := w.check(v.ID)
		if err != nil {
			l.WithError(err).Error("couldn't check video")

			if err := w.bs.Release(jobID, 10, time.Second*10); err != nil {
				l.WithError(err).Error("couldn't release job back to beanstalkd")
			}

			continue
		}

		l.Info("checker got video information")

		if mp4URL != "" {
			var validity time.Duration
			if v.Mp4URLExpires != nil {
				validity = v.Mp4URLExpires.Sub(time.Now())
			}

			expires := time.Now().Add(time.Hour * 4)

			if u, err := url.Parse(mp4URL); err == nil {
				if s := u.Query().Get("expire"); s != "" {
					if n, err := strconv.ParseInt(s, 10, 64); err == nil {
						expires = time.Unix(n, 0)
					}
				}
			}

			l := l.WithFields(logrus.Fields{"url_validity": validity})

			l.Info("checker got mp4 url")

			if _, err := w.db.Exec("update videos set last_seen_at = $1, mp4_url = $2, mp4_url_expires = $3 where id = $4", time.Now(), mp4URL, expires, v.ID); err != nil {
				l.WithError(err).Error("couldn't save video record")

				if err := w.bs.Release(jobID, 10, time.Second*10); err != nil {
					l.WithError(err).Error("couldn't release job back to beanstalkd")
				}

				continue
			}

			if _, err := w.db.Exec("insert into video_urls (id, video_id, seen_at, url, expires) values ($1, $2, $3, $4, $5)", uuid.Must(uuid.NewV4()).String(), v.ID, time.Now(), mp4URL, expires); err != nil {
				l.WithError(err).Error("couldn't insert video url record")

				if err := w.bs.Release(jobID, 10, time.Second*10); err != nil {
					l.WithError(err).Error("couldn't release job back to beanstalkd")
				}

				continue
			}

			delay := expires.Sub(time.Now()) - w.validityThreshold
			if delay < 0 {
				delay = 0
			}

			if _, err := ctube.Put(videoID, 0, delay, time.Second*10); err != nil {
				l.WithError(err).Error("couldn't insert repeat job")

				if err := w.bs.Release(jobID, 10, time.Second*10); err != nil {
					l.WithError(err).Error("couldn't release job back to beanstalkd")
				}

				continue
			}
		}

		if removalMessage != "" {
			var validity time.Duration
			if v.Mp4URLExpires != nil {
				validity = v.Mp4URLExpires.Sub(time.Now())
			}

			l := logrus.WithFields(logrus.Fields{"url_validity": validity, "removal_message": removalMessage})

			l.Warn("video has been removed")

			if _, err := w.db.Exec("update videos set removed_at = $1, removal_message = $2 where id = $3", time.Now(), removalMessage, v.ID); err != nil {
				l.WithError(err).Error("couldn't save video record")

				if err := w.bs.Release(jobID, 10, time.Second*10); err != nil {
					l.WithError(err).Error("couldn't release job back to beanstalkd")
				}

				continue
			}

			if !strings.Contains(strings.ToLower(removalMessage), "copyright") {
				if _, err := dtube.Put(videoID, 0, 0, time.Minute); err != nil {
					l.WithError(err).Error("couldn't insert downloader job")

					if err := w.bs.Release(jobID, 10, time.Second*10); err != nil {
						l.WithError(err).Error("couldn't release job back to beanstalkd")
					}
				}
			}
		}

		if err := w.bs.Delete(jobID); err != nil {
			l.WithError(err).Error("couldn't remove job from beanstalkd")
		}
	}
}

func (w *videoCheckerWorker) check(id string) (string, string, error) {
	info, err := ytdl.GetVideoInfoFromID(id)
	if err != nil {
		if str := err.Error(); strings.Contains(str, "Unavailable because:") {
			return "", regexp.MustCompile("^.*Unavailable because: ").ReplaceAllString(str, ""), nil
		}

		return "", "", err
	}

	formats := info.Formats.Filter(ytdl.FormatExtensionKey, []interface{}{"mp4"})
	if len(formats) == 0 {
		return "", "", errors.New("couldn't find mp4 download")
	}

	u, err := info.GetDownloadURL(formats[0])
	if err != nil {
		return "", "", err
	}

	return u.String(), "", nil
}
