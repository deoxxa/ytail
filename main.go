package main

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"time"

	"github.com/GeertJohan/go.rice"
	"github.com/beanstalkd/go-beanstalk"
	"github.com/fsnotify/fsnotify"
	_ "github.com/mattn/go-sqlite3"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type beanstalkdFactory func() (*beanstalk.Conn, error)

var (
	errStop   = fmt.Errorf("stop iterating")
	errNoJobs = fmt.Errorf("no jobs to run")
)

const (
	thumbnailDownloaderQueue = "thumbnail-downloader"
	videoCheckerQueue        = "video-checker"
	videoDownloaderQueue     = "video-downloader"
)

type beanstalkJob struct {
	id   uint64
	data []byte
}

type Config struct {
	LogLevel                      string                  `mapstructure:"log_level"`
	Database                      string                  `mapstructure:"database"`
	Beanstalkd                    string                  `mapstructure:"beanstalkd"`
	ClientSecretFile              string                  `mapstructure:"client_secret_file"`
	TokenCacheFile                string                  `mapstructure:"token_cache_file"`
	Address                       string                  `mapstructure:"address"`
	VideoCheckerMaxAge            time.Duration           `mapstructure:"video_checker_max_age"`
	VideoCheckerValidityThreshold time.Duration           `mapstructure:"video_checker_validity_threshold"`
	VideoCheckerWorkers           int                     `mapstructure:"video_checker_workers"`
	ThumbnailDownloadWorkers      int                     `mapstructure:"thumbnail_download_workers"`
	VideoDownloadWorkers          int                     `mapstructure:"video_download_workers"`
	Search                        map[string]SearchConfig `mapstructure:"search"`
}

type SearchConfig struct {
	Query    string        `mapstructure:"query"`
	Interval time.Duration `mapstructure:"interval"`
	Window   time.Duration `mapstructure:"window"`
}

func main() {
	viper.SetDefault("log_level", "info")
	viper.SetDefault("database", "database.db")
	viper.SetDefault("beanstalkd", "127.0.0.1:11300")
	viper.SetDefault("client_secret_file", "client_secret.json")
	viper.SetDefault("token_cache_file", "token_cache.json")
	viper.SetDefault("address", ":8000")
	viper.SetDefault("video_checker_max_age", time.Hour*72)
	viper.SetDefault("video_checker_validity_threshold", time.Hour*4)
	viper.SetDefault("video_checker_workers", 4)
	viper.SetDefault("thumbnail_download_workers", 4)
	viper.SetDefault("video_download_workers", 4)

	viper.SetConfigName("ytail")
	viper.AddConfigPath(".")
	if err := viper.ReadInConfig(); err != nil {
		panic(err)
	}

	var cfg Config
	if err := viper.Unmarshal(&cfg); err != nil {
		panic(err)
	}

	level, err := logrus.ParseLevel(cfg.LogLevel)
	if err != nil {
		panic(err)
	}
	logrus.SetLevel(level)

	db, err := sql.Open("sqlite3", cfg.Database)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	newBS := beanstalkdFactory(func() (*beanstalk.Conn, error) { return beanstalk.Dial("tcp", cfg.Beanstalkd) })

	bs, err := newBS()
	if err != nil {
		panic(err)
	}
	defer bs.Close()

	if _, err := migrate(context.Background(), db, rice.MustFindBox("migrations")); err != nil {
		panic(err)
	}

	if false {
		tube := beanstalk.Tube{Conn: bs, Name: videoCheckerQueue}

		rows, err := db.Query("select id from videos where removed_at is null order by mp4_url_expires asc, first_seen_at asc")
		if err != nil {
			panic(err)
		}
		defer rows.Close()

		for rows.Next() {
			var videoID string
			if err := rows.Scan(&videoID); err != nil {
				panic(err)
			}

			if _, err := tube.Put([]byte(videoID), 5, 0, time.Second*10); err != nil {
				panic(err)
			}
		}

		if err := rows.Close(); err != nil {
			panic(err)
		}
	}

	svc, err := getService(context.Background(), cfg.ClientSecretFile, cfg.TokenCacheFile)
	if err != nil {
		panic(err)
	}

	searchers := make(map[string]*searcher)
	for k, config := range cfg.Search {
		searchers[k] = newSearcher(db, bs, svc, config.Query, config.Interval, config.Window)
		go searchers[k].run()
		searchers[k].trigger()
	}

	thumbnailDownloader, err := newThumbnailDownloader(db, newBS, cfg.ThumbnailDownloadWorkers)
	if err != nil {
		panic(err)
	}

	videoDownloader, err := newVideoDownloader(db, newBS, cfg.VideoDownloadWorkers)
	if err != nil {
		panic(err)
	}

	videoChecker, err := newVideoChecker(db, newBS, cfg.VideoCheckerValidityThreshold, cfg.VideoCheckerMaxAge, cfg.VideoCheckerWorkers)
	if err != nil {
		panic(err)
	}

	viper.WatchConfig()
	viper.OnConfigChange(func(ev fsnotify.Event) {
		var newConfig Config
		if err := viper.Unmarshal(&newConfig); err != nil {
			logrus.WithError(err).Error("couldn't read new config; not updating settings")
			return
		}

		if level, err := logrus.ParseLevel(newConfig.LogLevel); err != nil {
			logrus.WithError(err).Error("couldn't parse log level option")
		} else {
			logrus.SetLevel(level)
			logrus.WithField("log_level", level).Debug("changing log level")
		}

		for k, searcher := range searchers {
			if _, ok := newConfig.Search[k]; !ok {
				go searcher.shutdown()
				delete(searchers, k)
				continue
			}
		}

		for k, config := range newConfig.Search {
			if searcher, ok := searchers[k]; !ok {
				searchers[k] = newSearcher(db, bs, svc, config.Query, config.Interval, config.Window)
				go searchers[k].run()
				searchers[k].trigger()
			} else if searcher.query != config.Query || searcher.interval != config.Interval || searcher.window != config.Window {
				searcher.query = config.Query
				searcher.interval = config.Interval
				searcher.window = config.Window
				searcher.trigger()
			}
		}

		videoChecker.setValidityThreshold(newConfig.VideoCheckerValidityThreshold)
		videoChecker.setMaxAge(newConfig.VideoCheckerMaxAge)

		if err := thumbnailDownloader.setWorkerCount(newConfig.ThumbnailDownloadWorkers); err != nil {
			logrus.WithError(err).Error("couldn't adjust worker count for thumbnail downloader")
		}
		if err := videoDownloader.setWorkerCount(newConfig.VideoDownloadWorkers); err != nil {
			logrus.WithError(err).Error("couldn't adjust worker count for video downloader")
		}
		if err := videoChecker.setWorkerCount(newConfig.VideoCheckerWorkers); err != nil {
			logrus.WithError(err).Error("couldn't adjust worker count for video checker")
		}
	})

	server := NewServer(db, bs)
	if err := http.ListenAndServe(cfg.Address, server); err != nil {
		panic(err)
	}
}
