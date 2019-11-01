create table "videos" (
  id text not null primary key,
  published_at datetime not null,
  first_seen_at datetime not null,
  last_seen_at datetime not null,
  title text not null,
  description text not null,
  channel_id text not null,
  channel_title text not null,
  thumbnail_url text not null,
  mp4_url text,
  mp4_url_expires datetime,
  removed_at datetime,
  removal_message text,
  thumbnail_downloaded_at datetime,
  video_downloaded_at datetime
);

create table video_urls (
  id text not null primary key,
  video_id text not null references videos (id),
  seen_at datetime not null,
  url text not null,
  expires datetime not null
);
