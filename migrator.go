package main

import (
	"context"
	"database/sql"
	"os"
	"regexp"
	"sort"
	"time"

	"github.com/GeertJohan/go.rice"
	"github.com/sirupsen/logrus"
	"github.com/pkg/errors"
)

func migrate(ctx context.Context, db *sql.DB, box *rice.Box) (bool, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return false, errors.Wrap(err, "migrate: couldn't start tx")
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `create table if not exists migrations (id integer not null primary key, name text not null unique, applied_at timestamp not null);`); err != nil {
		return false, errors.Wrap(err, "migrate: couldn't create migrations table")
	}

	var names []string
	if err := box.Walk("", func(p string, m os.FileInfo, err error) error {
		if err != nil {
			return errors.Wrap(err, "migrate: couldn't read box content")
		}

		if matched, _ := regexp.MatchString(`^[0-9]+_.+?\.sql$`, p); matched {
			names = append(names, p)
		}

		return nil
	}); err != nil {
		return false, errors.Wrap(err, "migrate: couldn't collect file names")
	}
	sort.Strings(names)

	rows, err := tx.QueryContext(ctx, "select name from migrations")
	if err != nil {
		return false, errors.Wrap(err, "migrate: couldn't perform applied migration list query")
	}
	defer rows.Close()

	applied := make(map[string]bool)

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return false, errors.Wrap(err, "migrate: couldn't scan applied migration name")
		}

		applied[name] = true
	}

	var changed bool

	for _, n := range names {
		s, err := box.String(n)
		if err != nil {
			return false, errors.Wrapf(err, "migrate: couldn't get migration content for %q", n)
		}

		if applied[n] {
			continue
		}

		var count int
		if err := tx.QueryRowContext(ctx, "select count(1) from migrations where name = $1", n).Scan(&count); err != nil {
			return false, err
		} else if count == 0 {
			logrus.WithField("file", n).Info("applying migration")

			if _, err := tx.ExecContext(ctx, s); err != nil {
				return false, errors.Wrapf(err, "migrate: couldn't run migration %q", n)
			}

			if _, err = tx.ExecContext(ctx, "insert into migrations (name, applied_at) values($1, $2)", n, time.Now()); err != nil {
				return false, errors.Wrapf(err, "migrate: couldn't record completion for %q", n)
			}

			changed = true
		} else {
			logrus.WithField("file", n).Info("already applied")
		}
	}

	if err := tx.Commit(); err != nil {
		return false, errors.Wrap(err, "migrate: couldn't commit transaction")
	}

	return changed, nil
}
