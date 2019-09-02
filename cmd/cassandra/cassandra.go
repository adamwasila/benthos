package main

import (
	"fmt"
	"log"
	"time"

	"github.com/gocql/gocql"
)

func main() {
	// connect to the cluster
	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Keyspace = "benthos"
	// cluster.Consistency = gocql.One
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: "cassandra",
		Password: "cassandra",
	}

	erp := &gocql.ExponentialBackoffRetryPolicy{
		NumRetries: 3,
		Min:        3 * time.Second,
		Max:        10 * time.Second,
	}
	cluster.RetryPolicy = &delegate{
		rq: erp,
	}

	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("connecting error: %v", err)
		return
	}
	defer session.Close()

	if err := session.Query(`INSERT INTO benthos.x (col1, col2) VALUES (?, ?)`, "hello world", gocql.TimeUUID()).Exec(); err != nil {
		log.Fatalf(">>> %v", err)
	}

}

type delegate struct {
	rq gocql.RetryPolicy
}

func (d *delegate) Attempt(q gocql.RetryableQuery) bool {
	r := d.rq.Attempt(q)
	fmt.Printf("Attempt %d %v\n\n", q.Attempts(), r)
	return r
}

func (d *delegate) GetRetryType(err error) gocql.RetryType {
	fmt.Printf("retry type: %v\n\n", err)
	return gocql.Retry
}
