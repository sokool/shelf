package es

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type MySQL struct {
	db *sql.DB
}

func (s *MySQL) Append(a AggregateEvents, expectedVersion uint) error {
	v := s.version(a.Aggregate)
	now := time.Now()
	nowf := now.Format("2006-01-02 15:04:05")
	//todo expectedVersion need to be determined by mysql and compared (eventual consistency)
	stmt, err := s.db.Prepare(insertEvent)
	if err != nil {
		return err
	}

	defer stmt.Close()

	for i, e := range a.Events {
		v++
		a.Events[i].Version = v
		a.Events[i].CreatedAt = now

		_, err := stmt.Exec(a.ID, string(a.Type), string(e.Type), v, nowf, e.Data, e.Meta)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *MySQL) FromVersion(a Aggregate, v uint) (AggregateEvents, error) {
	out := AggregateEvents{}

	var r *sql.Rows
	var err error

	r, err = s.db.Query("SELECT * FROM cqrs_events WHERE aggregate_id = ? AND aggregate_name = ? AND sequence >= ? ORDER BY sequence ASC",
		a.ID, a.Type, v)

	if err != nil {
		return out, err
	}

	defer r.Close()

	for r.Next() {
		var t string
		e := Event{}
		if err := r.Scan(&out.ID, &out.Type, &e.Type, &e.Version, &t, &e.Data, &e.Meta); err != nil {
			return out, err
		}

		out.Events = append(out.Events, e)
	}

	if err = r.Err(); err != nil {
		return out, err
	}

	return out, nil
}

func (s *MySQL) All(aggregate string) ([]AggregateEvents, error) {
	return s.all(`
	SELECT *
		FROM cqrs_events
		WHERE aggregate_name = ?
		ORDER BY
			aggregate_id,
			sequence ASC`, aggregate)
}

func (s *MySQL) Copy(aggregate, src string, after uint, dst string) error {
	q := `INSERT INTO cqrs_events (aggregate_id, aggregate_name, name, sequence, created_at, payload, meta) 
			SELECT ?, aggregate_name, name, sequence, created_at, payload, meta 
				FROM cqrs_events 
				WHERE 
					aggregate_id = ? 
					AND aggregate_name = ? 
					AND sequence > ?`

	stmt, err := s.db.Prepare(q)
	if err != nil {
		return err
	}

	defer stmt.Close()

	if _, err := stmt.Exec(dst, src, aggregate, after); err != nil {
		return err
	}

	return nil
}

func (s *MySQL) Create(overwrite ...bool) error {
	if len(overwrite) == 1 && overwrite[0] {
		stmt, err := s.db.Prepare("DROP TABLE IF EXISTS cqrs_events;")
		if err != nil {
			return err
		}

		defer stmt.Close()
		_, err = stmt.Exec()
		if err != nil {
			return err
		}
	}

	stmt, err := s.db.Prepare(createEventsTable)
	if err != nil {
		return err
	}

	defer stmt.Close()
	_, err = stmt.Exec()

	return err
}

func (s *MySQL) version(a Aggregate) uint {
	var version uint
	v := s.db.QueryRow("SELECT sequence FROM cqrs_events WHERE aggregate_id = ? AND aggregate_name = ? ORDER BY sequence DESC LIMIT 1", a.ID, a.Type)

	if err := v.Scan(&version); err == sql.ErrNoRows {
		return 0
	} else if err != nil {
		fmt.Printf("ups, loading aggregate version from mysql failed! (%s)\n", err)
		return 0
	}

	return version
}

func (s *MySQL) all(query string, args ...interface{}) ([]AggregateEvents, error) {
	r, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}

	defer r.Close()

	tmp := make(map[string]*AggregateEvents)
	for r.Next() {
		var id, name, t string
		e := Event{}
		if err := r.Scan(&id, &name, &e.Type, &e.Version, &t, &e.Data, &e.Meta); err != nil {
			return nil, err
		}

		d, _ := time.Parse("2006-01-02 15:04:05", t)
		e.CreatedAt = d

		a, ok := tmp[id]
		if !ok {
			a = &AggregateEvents{
				Aggregate: Aggregate{
					ID:   id,
					Type: name,
				}}

			tmp[id] = a
		}

		a.Events = append(a.Events, e)
	}

	if err = r.Err(); err != nil {
		return nil, err
	}

	var events []AggregateEvents
	for i := range tmp {
		events = append(events, *tmp[i])
	}

	return events, nil
}

//func (s *EventStore) Stream([]piper.Query) ([]piper.Event, error) {
//	panic("implement me")
//}
//
//func (s *EventStore) Append(id, aggregate string, expectedVersion uint, events []piper.Event) error {
//	stmt, err := s.db.Prepare(insertEvent)
//	if err != nil {
//		return err
//	}
//
//	defer stmt.Close()
//
//	for _, e := range events {
//		_, err := stmt.Exec(e.AggregateID, e.AggregateName, e.Name, e.AggregateVersion, e.Created.Format("2006-01-02 15:04:05"), e.Data, e.Meta)
//		if err != nil {
//			return err
//		}
//	}
//
//	return nil
//}
//
//func (s *EventStore) Read(id, aggregate string, fromVersion uint) ([]piper.Event, error) {
//	var events []piper.Event
//
//	var r *sql.Rows
//	var err error
//
//	if id != "" {
//		r, err = s.db.Query("SELECT * FROM cqrs_events WHERE aggregate_id = ? AND aggregate_name = ? AND sequence >= ? ORDER BY sequence ASC",
//			id, aggregate, fromVersion)
//	} else {
//		r, err = s.db.Query("SELECT * FROM cqrs_events WHERE aggregate_name = ? AND sequence >= ? ORDER BY sequence ASC",
//			aggregate, fromVersion)
//	}
//
//	if err != nil {
//		return events, err
//	}
//
//	defer r.Close()
//
//	for r.Next() {
//		var t string
//		e := piper.Event{}
//		if err := r.Scan(&e.AggregateID, &e.AggregateName, &e.Name, &e.AggregateVersion, &t, &e.Data, &e.Meta); err != nil {
//			return events, err
//		}
//
//		events = append(events, e)
//	}
//
//	if err = r.Err(); err != nil {
//		return events, err
//	}
//
//	return events, nil
//}
//
//func (s *EventStore) Event(version uint) (piper.Event, error) {
//	panic("implement me")
//}
//
//func (s *EventStore) ByDate(id, aggregate string, from time.Time) ([]piper.Event, error) {
//	var query string
//	var args []interface{}
//
//	if id == "" {
//		query = `
//		SELECT *
//			FROM cqrs_events
//			WHERE aggregate_name = ?
//			AND created_at >= ?
//		ORDER BY aggregate_id, sequence ASC`
//		args = []interface{}{aggregate, from}
//	} else {
//		query = `
//		SELECT *
//			FROM cqrs_events
//			WHERE aggregate_name = ?
//			AND aggregate_id = ?
//			AND created_at >= ?
//		ORDER BY sequence ASC`
//		args = []interface{}{aggregate, id, from}
//	}
//
//	return s.all(query, args...)
//}

func NewMySQL(c *sql.DB) *MySQL {
	return &MySQL{
		db: c,
	}
}

const insertEvent = `INSERT INTO
	cqrs_events(aggregate_id, aggregate_name, name, sequence, created_at, payload, meta)
	VALUES(?, ?, ?, ?, ?, ?, ?)`

const createEventsTable = `CREATE TABLE IF NOT EXISTS cqrs_events (
  aggregate_id varchar(255) NOT NULL,
  aggregate_name varchar(255) NOT NULL,
  name varchar(255) NOT NULL,
  sequence int(11) NOT NULL,
  created_at varchar(255) NOT NULL,
  payload TEXT NOT NULL,
  meta text,
  PRIMARY KEY (aggregate_id, aggregate_name, sequence)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;`
