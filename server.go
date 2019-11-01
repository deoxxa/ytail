package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"fknsrs.biz/p/sorm"
	"fknsrs.biz/p/sorm/qsorm"
	sb "fknsrs.biz/p/sqlbuilder"
	"github.com/beanstalkd/go-beanstalk"
	"github.com/sirupsen/logrus"
)

type Server struct {
	db *sql.DB
	bs *beanstalk.Conn
}

func NewServer(db *sql.DB, bs *beanstalk.Conn) *Server {
	return &Server{db: db, bs: bs}
}

func (s *Server) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	defer func() {
		if ex := recover(); ex != nil {
			rw.Header().Set("content-type", "text/plain")
			rw.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(rw, "%v\n", ex)
		}
	}()

	logrus.WithFields(logrus.Fields{
		"method": r.Method,
		"uri":    r.RequestURI,
	}).Info("http request")

	thumbsFS := http.StripPrefix("/thumbs", http.FileServer(http.Dir("thumbs")))
	videosFS := http.StripPrefix("/videos", http.FileServer(http.Dir("videos")))

	switch {
	case r.URL.Path == "/":
		s.serveIndex(rw, r)
	case r.URL.Path == "/queues":
		s.serveQueues(rw, r)
	case r.URL.Path == "/counts":
		s.serveCounts(rw, r)
	case r.URL.Path == "/removed":
		s.serveRemoved(rw, r)
	case r.URL.Path == "/play":
		s.servePlay(rw, r)
	case strings.HasPrefix(r.URL.Path, "/thumbs/"):
		thumbsFS.ServeHTTP(rw, r)
	case strings.HasPrefix(r.URL.Path, "/videos/"):
		videosFS.ServeHTTP(rw, r)
	default:
		http.Error(rw, "", http.StatusNotFound)
	}
}

func (s *Server) serveIndex(rw http.ResponseWriter, r *http.Request) {
	rw.Header().Set("content-tye", "text/html")
	rw.WriteHeader(http.StatusOK)
	fmt.Fprintf(rw, `<a href="/counts">counts</a>`)
	fmt.Fprintf(rw, `<a href="/removed">removed</a>`)
}

func (s *Server) serveQueues(rw http.ResponseWriter, r *http.Request) {
	names, err := s.bs.ListTubes()
	if err != nil {
		panic(err)
	}

	sort.Strings(names)

	rw.Header().Set("content-type", "text/plain")
	rw.WriteHeader(http.StatusOK)

	for _, name := range names {
		fmt.Fprintf(rw, "queue: %s\n", name)

		tube := beanstalk.Tube{Conn: s.bs, Name: name}

		stats, err := tube.Stats()
		if err != nil {
			panic(err)
		}

		enc := json.NewEncoder(rw)
		enc.SetIndent("", "  ")
		if err := enc.Encode(stats); err != nil {
			panic(err)
		}

		fmt.Fprintf(rw, "\n")
	}
}

func (s *Server) serveCounts(rw http.ResponseWriter, r *http.Request) {
	rows, err := s.db.QueryContext(r.Context(), `select ((mp4_url_expires is null or mp4_url_expires < current_timestamp) and video_downloaded_at is null) as need_url, removed_at is not null as is_removed, count(*) as n from videos group by need_url, is_removed`)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	var atRisk, missedURL, monitoring, maybeCaught int
	for rows.Next() {
		var needURL, isRemoved bool
		var count int
		if err := rows.Scan(&needURL, &isRemoved, &count); err != nil {
			panic(err)
		}

		switch {
		case needURL && !isRemoved:
			atRisk = count
		case needURL && isRemoved:
			missedURL = count
		case !needURL && !isRemoved:
			monitoring = count
		case !needURL && isRemoved:
			maybeCaught = count
		}
	}

	if err := rows.Close(); err != nil {
		panic(err)
	}

	var minTime, maxTime, avgTime float64
	if err := s.db.QueryRowContext(r.Context(), "select min(expired), max(expired), avg(expired) from (select strftime('%s', mp4_url_expires) - strftime('%s', $1) as expired from videos where mp4_url_expires is not null and removed_at is null)", time.Now()).Scan(&minTime, &maxTime, &avgTime); err != nil {
		panic(err)
	}

	var videoCount int
	if err := s.db.QueryRowContext(r.Context(), "select count(*) from videos").Scan(&videoCount); err != nil {
		panic(err)
	}

	var videoURLCount int
	if err := s.db.QueryRowContext(r.Context(), "select count(*) from video_urls").Scan(&videoURLCount); err != nil {
		panic(err)
	}

	rw.Header().Set("content-type", "text/plain")
	rw.WriteHeader(http.StatusOK)
	fmt.Fprintf(
		rw,
		"video records: %d\nvideo url records: %d\nat risk: %d\nmissed url: %d\nmonitoring: %d\nmaybe caught: %d\nmin/max/avg: %s/%s/%s",
		videoCount, videoURLCount, atRisk, missedURL, monitoring, maybeCaught, time.Duration(minTime*float64(time.Second)), time.Duration(maxTime*float64(time.Second)), time.Duration(avgTime*float64(time.Second)),
	)
}

func (s *Server) serveRemoved(rw http.ResponseWriter, r *http.Request) {
	where := []sb.AsExpr{sb.IsNotNull(videosTable.C("removed_at"))}

	if s := r.URL.Query().Get("q"); s != "" {
		where = append(where, sb.BooleanOperator("or",
			sb.Like(videosTable.C("channel_title"), sb.Bind("%"+s+"%")),
			sb.Like(videosTable.C("title"), sb.Bind("%"+s+"%")),
			sb.Like(videosTable.C("removal_message"), sb.Bind("%"+s+"%")),
		))
	}

	if r.URL.Query().Get("missed") != "true" {
		where = append(where, sb.IsNotNull(videosTable.C("video_downloaded_at")))
	}

	total, err := qsorm.CountWhere(r.Context(), s.db, &video{}, sb.BooleanOperator("and", where...))
	if err != nil {
		panic(err)
	}

	page := 0
	if n, err := strconv.ParseInt(r.URL.Query().Get("page"), 10, 64); err == nil {
		page = int(n)
	}
	if page < 0 {
		page = 0
	}

	var pages []int
	for i := 0; i <= total/20; i++ {
		pages = append(pages, i+1)
	}

	if page == 0 {
		page = pages[len(pages)-1]
	}

	var records []video
	if err := qsorm.FindWhere(
		r.Context(),
		s.db,
		&records,
		sb.BooleanOperator("and", where...),
		[]sb.AsOrderingTerm{sb.OrderAsc(videosTable.C("removed_at"))},
		sb.OffsetLimit(sb.Bind((page-1)*20), sb.Bind(20)),
	); err != nil {
		panic(err)
	}

	tpl := template.Must(template.New("").Parse(`
{{$Root := .}}

<style>
#search > div {
  text-align: center;
}

.pagination {
  margin: 1em 0;
}

.pagination a,
.pagination span {
  padding: 3px 5px;
  margin-right: 5px;
  border: 1px dashed #ddd;
}

.pagination span {
  border: 1px solid #ddd;
  background: #eee;
}

.container {
  display: flex;
  justify-content: space-around;
  flex-wrap: wrap;
}

.video {
  flex: 1 1 20%;
  padding: 1em;
  border: 1px dashed #ddd;
}

.video > img {
  max-width: 100%;
  margin: auto;
}
</style>

<form method="get" action="/removed" id="search">
  <div>
    <input name="q" value="{{$Root.Query}}" />
    <input type="submit" value="Search" />
  </div>
</form>

<div class="pagination">
{{range $p := $Root.Pages}}
{{if (eq $p $Root.Page)}}
  <span>{{$p}}</span>
{{else}}
  <a href="/removed?q={{$Root.Query}}&page={{$p}}">{{$p}}</a>
{{end}}
{{end}}
</div>

<div class="container">
{{range $r := $Root.Records}}
  <div class="video">
    {{if $r.VideoDownloadedAt}}
    <a href="/play?id={{$r.ID}}">
      <img src="/thumbs/{{$r.ID}}.jpg" alt="{{$r.ID}}" />
    </a>
    {{else}}
    <img src="/thumbs/{{$r.ID}}.jpg" alt="{{$r.ID}}" />
    {{end}}
    <br />
    <a href="https://www.youtube.com/channel/{{$r.ChannelID}}">{{$r.ChannelTitle}} ({{$r.ChannelID}})</a>
    <br />
    <a href="https://www.youtube.com/watch?v={{$r.ID}}">{{$r.Title}} ({{$r.ID}})</a>
    <br />
    <span>Seen from {{$r.FirstSeenAt.Format "2006-01-02 15:04:05"}} for {{$r.LastSeenAt.Sub $r.FirstSeenAt}}</span>
    {{if $r.RemovedAt}}
      <br />
      <span>Removed at {{$r.RemovedAt.Format "2006-01-02 15:04:05"}} ({{$r.RemovalMessage}})</span>
    {{end}}
    {{if $r.VideoDownloadedAt}}
      <br />
      <span>Downloaded at {{$r.VideoDownloadedAt.Format "2006-01-02 15:04:05"}}</span>
      <br />
      <a href="/play?id={{$r.ID}}">Play video</a>
    {{end}}
  </div>
{{end}}
</div>

<div class="pagination">
{{range $p := $Root.Pages}}
{{if (eq $p $Root.Page)}}
  <span>{{$p}}</span>
{{else}}
  <a href="/removed?q={{$Root.Query}}&page={{$p}}">{{$p}}</a>
{{end}}
{{end}}
</div>
  `))

	rw.Header().Set("content-type", "text/html;charset=utf-8")
	rw.WriteHeader(http.StatusOK)
	if err := tpl.Execute(rw, map[string]interface{}{
		"Records": records,
		"Total":   total,
		"Pages":   pages,
		"Page":    page,
		"Query":   r.URL.Query().Get("q"),
	}); err != nil {
		panic(err)
	}
}

func (s *Server) servePlay(rw http.ResponseWriter, r *http.Request) {
	var v video
	if err := sorm.FindFirstWhere(r.Context(), s.db, &v, "where id = $1", r.URL.Query().Get("id")); err != nil {
		panic(err)
	}

	tpl := template.Must(template.New("").Parse(`
{{$Root := .}}

<style>
.container {
  margin: 1em;
}

.player {
  width: 100%;
  max-height: 600px;
}

.info {
  margin-top: 1em;
}
</style>

<div class="container">
  <video class="player" controls>
    <source src="/videos/{{$Root.Video.ID}}.mp4" type="video/mp4">
  </video>

  <div class="info">
    Channel: <a href="https://www.youtube.com/channel/{{$Root.Video.ChannelID}}">{{$Root.Video.ChannelTitle}} ({{$Root.Video.ChannelID}})</a>
    <br />
    Video link: <a href="https://www.youtube.com/watch?v={{$Root.Video.ID}}">{{$Root.Video.Title}} ({{$Root.Video.ID}})</a>
    <br />
    <span>Seen from {{$Root.Video.FirstSeenAt.Format "2006-01-02 15:04:05"}} for {{$Root.Video.LastSeenAt.Sub $Root.Video.FirstSeenAt}}</span>
    {{if $Root.Video.RemovedAt}}
      <br />
      <span>Removed at {{$Root.Video.RemovedAt.Format "2006-01-02 15:04:05"}} ({{$Root.Video.RemovalMessage}})</span>
    {{end}}
    {{if $Root.Video.VideoDownloadedAt}}
      <br />
      <span>Video Downloaded at {{$Root.Video.VideoDownloadedAt.Format "2006-01-02 15:04:05"}}</span>
      <br />
      <a href="/videos/{{$Root.Video.ID}}.mp4" download>Download video</a>
    {{end}}
  </div>
</div>
  `))

	rw.Header().Set("content-type", "text/html;charset=utf-8")
	rw.WriteHeader(http.StatusOK)
	if err := tpl.Execute(rw, map[string]interface{}{
		"Video": v,
	}); err != nil {
		panic(err)
	}
}
