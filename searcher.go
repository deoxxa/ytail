package main

import (
	"context"
	"database/sql"
	"time"

	"github.com/beanstalkd/go-beanstalk"
	"github.com/sirupsen/logrus"
	"google.golang.org/api/youtube/v3"
)

type searcher struct {
	db          *sql.DB
	thumbsTube  *beanstalk.Tube
	checkerTube *beanstalk.Tube
	service     *youtube.Service
	query       string
	interval    time.Duration
	window      time.Duration
	poke        chan struct{}
	stop        chan chan<- struct{}
}

func newSearcher(db *sql.DB, bs *beanstalk.Conn, service *youtube.Service, query string, interval, window time.Duration) *searcher {
	return &searcher{
		db:          db,
		thumbsTube:  &beanstalk.Tube{Conn: bs, Name: thumbnailDownloaderQueue},
		checkerTube: &beanstalk.Tube{Conn: bs, Name: videoCheckerQueue},
		service:     service,
		query:       query,
		interval:    interval,
		window:      window,
		poke:        make(chan struct{}),
		stop:        make(chan chan<- struct{}),
	}
}

func (s *searcher) trigger() {
	logrus.WithFields(logrus.Fields{
		"query":    s.query,
		"interval": s.interval,
		"window":   s.window,
	}).Info("triggering searcher")

	s.poke <- struct{}{}
}

func (s *searcher) shutdown() {
	logrus.WithFields(logrus.Fields{
		"query":    s.query,
		"interval": s.interval,
		"window":   s.window,
	}).Info("stopping searcher")

	ch := make(chan struct{})
	s.stop <- ch
	<-ch
}

func (s *searcher) run() {
	logrus.WithFields(logrus.Fields{
		"query":    s.query,
		"interval": s.interval,
		"window":   s.window,
	}).Info("running searcher")

loop:
	for {
		select {
		case <-s.stop:
			break loop
		case <-s.poke:
			if err := s.search(); err != nil {
				logrus.WithError(err).Warn("search failed")
			}
		case <-time.After(s.interval):
			if err := s.search(); err != nil {
				logrus.WithError(err).Warn("search failed")
			}
		}
	}
}

func (s *searcher) search() error {
	c := s.service.Search.List("snippet")

	if s.query != "" {
		c = c.Q(s.query)
	}
	c = c.Type("video")
	c = c.PublishedAfter(time.Now().Add(0 - s.window).Format(time.RFC3339))
	c = c.Order("date")

	total := 0
	found := 0

	if err := c.Pages(context.Background(), func(res *youtube.SearchListResponse) error {
		for _, e := range res.Items {
			total++

			exists := false
			if err := s.db.QueryRow("select count(*) from videos where id = $1", e.Id.VideoId).Scan(&exists); err != nil {
				return err
			}

			if !exists {
				found++

				logrus.WithFields(logrus.Fields{
					"channel": e.Snippet.ChannelId,
					"id":      e.Id.VideoId,
					"query":   s.query,
				}).Debug("found new video")

				var thumbnailURL string
				if e.Snippet.Thumbnails != nil {
					if e.Snippet.Thumbnails.High != nil {
						thumbnailURL = e.Snippet.Thumbnails.High.Url
					} else if e.Snippet.Thumbnails.Medium != nil {
						thumbnailURL = e.Snippet.Thumbnails.Medium.Url
					} else if e.Snippet.Thumbnails.Default != nil {
						thumbnailURL = e.Snippet.Thumbnails.Default.Url
					}
				}

				now := time.Now()

				if _, err := s.db.Exec(
					"insert into videos (id, published_at, first_seen_at, last_seen_at, title, description, channel_id, channel_title, thumbnail_url) values ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
					e.Id.VideoId,
					e.Snippet.PublishedAt,
					now,
					now,
					e.Snippet.Title,
					e.Snippet.Description,
					e.Snippet.ChannelId,
					e.Snippet.ChannelTitle,
					thumbnailURL,
				); err != nil {
					return err
				}

				if _, err := s.thumbsTube.Put([]byte(e.Id.VideoId), 0, 0, time.Second*10); err != nil {
					logrus.WithError(err).Error("couldn't insert thumbnail job")
					return err
				}

				if _, err := s.checkerTube.Put([]byte(e.Id.VideoId), 0, 0, time.Second*10); err != nil {
					logrus.WithError(err).Error("couldn't insert checker job")
					return err
				}
			}
		}

		if len(res.Items) != int(res.PageInfo.ResultsPerPage) {
			return errStop
		}

		return nil
	}); err != nil && err != errStop {
		logrus.WithError(err).Error("an error occurred fetching search results")
		return err
	}

	logrus.WithFields(logrus.Fields{"total": total, "found": found}).Info("search run complete")

	return nil
}
