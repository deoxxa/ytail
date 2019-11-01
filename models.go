package main

import (
	"time"

	sb "fknsrs.biz/p/sqlbuilder"
)

type video struct {
	ID                    string     `json:"id" sql:"id"`
	PublishedAt           time.Time  `json:"publishedAt" sql:"published_at"`
	FirstSeenAt           time.Time  `json:"firstSeenAt" sql:"first_seen_at"`
	LastSeenAt            time.Time  `json:"lastSeenAt" sql:"last_seen_at"`
	Title                 string     `json:"title" sql:"title"`
	Description           string     `json:"description" sql:"description"`
	ChannelID             string     `json:"channelId" sql:"channel_id"`
	ChannelTitle          string     `json:"channelTitle" sql:"channel_title"`
	ThumbnailURL          string     `json:"thumbnailUrl" sql:"thumbnail_url"`
	ThumbnailDownloadedAt *time.Time `json:"thumbnailDownloadedAt" sql:"thumbnail_downloaded_at"`
	Mp4URL                *string    `json:"mp4Url" sql:"mp4_url"`
	Mp4URLExpires         *time.Time `json:"mp4UrlExpires" sql:"mp4_url_expires"`
	RemovedAt             *time.Time `json:"removedAt" sql:"removed_at"`
	RemovalMessage        *string    `json:"removalMessage" sql:"removal_message"`
	VideoDownloadedAt     *time.Time `json:"videoDownloadedAt" sql:"video_downloaded_at"`
}

type videoURL struct {
	ID      string    `json:"id" sql:"id"`
	VideoID string    `json:"videoId" sql:"video_id"`
	SeenAt  time.Time `json:"seenAt" sql:"seen_at"`
	URL     string    `json:"url" sql:"url"`
	Expires time.Time `json:"expires" sql:"expires"`
}

var (
	videosTable = sb.NewTable(
		"videos",
		"id",
		"published_at",
		"first_seen_at",
		"last_seen_at",
		"title",
		"description",
		"channel_id",
		"channel_title",
		"thumbnail_url",
		"thumbnail_downloaded_at",
		"mp4_url",
		"mp4_url_expires",
		"removed_at",
		"removal_message",
		"video_downloaded_at",
	)

	videoURLsTable = sb.NewTable(
		"video_urls",
		"id",
		"video_id",
		"seen_at",
		"url",
		"expires",
	)
)
