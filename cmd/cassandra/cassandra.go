package main

import (
	"fmt"
	"log"
	"math"
	"math/rand"
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

	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("connecting error: %v", err)
		return
	}
	defer session.Close()

	if err := session.Query(`INzERT INTO benthos.x (col1, col2) VALUES (?, ?)`, "hello world", gocql.TimeUUID()).Exec(); err != nil {
		log.Fatalf(">>> %v", err)
	}

}
