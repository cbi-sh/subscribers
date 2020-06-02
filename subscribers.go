package subscribers

import (
	"encoding/json"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"time"

	"github.com/cbi-sh/metrics"
	"github.com/gocql/gocql"
	"github.com/gorilla/mux"
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/gocqlx/qb"
)

const hosts = "127.0.0.1"
const keySpace = "cbi_sh"
const tableName = keySpace + "." + "subscribers"
const replicas = "3"
const primaryKey = "msisdn"

func connect(hosts ...string) *gocql.Session {

	cluster := gocql.NewCluster(hosts...)
	cluster.Consistency = gocql.One

	s, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}

	return s
}

func ExecStmt(s *gocql.Session, stmt string) error {
	q := s.Query(stmt).RetryPolicy(nil)
	defer q.Release()
	return q.Exec()
}

var session = connect(hosts)

func init() {

	if err := ExecStmt(session, keyspaceSchema); err != nil {
		log.Fatal(err)
	}

	if err := ExecStmt(session, subscriberSchema); err != nil {
		log.Fatal(err)
	}

	for msisdn := int64(380_11_000_0000); msisdn <= int64(380_11_000_0099); msisdn++ {

		subscriber := &Subscriber{
			Msisdn:         msisdn,
			ChangeDate:     time.Now(),
			LanguageType:   int8(rand.Int31n(6)),
			MigrationType:  int8(rand.Int31n(2)),
			OperatorType:   int8(rand.Int31n(6)),
			StateType:      int8(rand.Int31n(6)),
			SubscriberType: int8(rand.Int31n(2)),
		}

		if err := Set(subscriber); err != nil {
			log.Fatal("cannot set test subscriber, msisdn:", msisdn, err)
		}
	}
}

type Subscriber struct {
	Msisdn         int64     `json:"-"`
	ChangeDate     time.Time `json:"-"`
	LanguageType   int8      `json:"languageType,omitempty"`
	MigrationType  int8      `json:"migrationType,omitempty"`
	OperatorType   int8      `json:"operatorType,omitempty"`
	StateType      int8      `json:"stateType,omitempty"`
	SubscriberType int8      `json:"subscriberType,omitempty"`
}

// func (s *Subscriber) save() error {
// 	filename := strconv.FormatInt(s.Msisdn, 10) + ".txt"
// 	return ioutil.WriteFile(filename, s.Body, 0600)
// }
//
// func load(title string) *Subscriber {
// 	filename := title + ".txt"
// 	body, _ := ioutil.ReadFile(filename)
// 	return &Page{Title: title, Body: body}
// }

var subscriberSchema = `
CREATE TABLE IF NOT EXISTS ` + tableName +
	` (
       Msisdn BIGINT PRIMARY KEY, 
       Changedate TIMESTAMP,
       Languagetype TINYINT,
       Migrationtype TINYINT,
       Operatortype TINYINT,
       Statetype TINYINT,
       Subscribertype TINYINT,
      )`

var keyspaceSchema = `
CREATE KEYSPACE IF NOT EXISTS ` + keySpace +
	` WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': ` + replicas + `}`

var sStmt, sNames = qb.Select(tableName).Where(qb.Eq(primaryKey)).ToCql()
var sQuery = gocqlx.Query(session.Query(sStmt), sNames)

func Get(id int64) (*Subscriber, error) {
	var values []*Subscriber
	if err := gocqlx.Select(&values, sQuery.BindMap(qb.M{primaryKey: id}).Query); err != nil {
		return nil, err
	} else if len(values) == 0 {
		return nil, nil
	} else {
		return values[0], nil
	}
}

var uStmt, uNames = qb.Update(tableName).Where(qb.Eq(primaryKey)).ToCql()
var uQuery = gocqlx.Query(session.Query(uStmt), uNames)

func Set(value *Subscriber) error {

	stmt, names := qb.Insert(tableName).Columns("msisdn", "languagetype", "migrationtype", "operatortype", "statetype", "subscribertype").ToCql()
	q := gocqlx.Query(session.Query(stmt), names)
	defer q.Release()

	return q.BindStruct(value).Exec()
}

// ================================================================================================

func GetSubscriber(w http.ResponseWriter, r *http.Request) {

	if id, err := strconv.ParseInt(mux.Vars(r)["id"], 10, 64); err != nil {

		metrics.Inc400()
		w.WriteHeader(400)
		_ = json.NewEncoder(w).Encode(err)

	} else if subscriber, err := Get(id); err != nil {

		metrics.Inc500()
		w.WriteHeader(500)
		_ = json.NewEncoder(w).Encode(err)

	} else if subscriber == nil {

		metrics.Inc404()
		w.WriteHeader(404)

	} else {

		metrics.Inc200()
		w.WriteHeader(200)
		_ = json.NewEncoder(w).Encode(subscriber)

	}
}
