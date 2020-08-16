package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	neo4j18 "github.com/neo4j/neo4j-go-driver/neo4j"
	neo4j "github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

func setupReady(driver neo4j.Driver) bool {
	// Check if setup already built
	sess, err := driver.NewSession(neo4j.SessionConfig{})
	if err != nil {
		panic(err)
	}
	defer sess.Close()

	result, err := sess.Run("MATCH (s:Setup) RETURN s", nil)
	if err != nil {
		panic(err)
	}
	if !result.Next() {
		if err = result.Err(); err != nil {
			panic(err)
		}
		return false
	}
	return true
}

var numIntNodes = 1000
var numIntProps = 500

func buildSetup(driver neo4j.Driver) {
	fmt.Println("Building setup...")

	sess, err := driver.NewSession(neo4j.SessionConfig{})
	if err != nil {
		panic(err)
	}
	defer sess.Close()

	// Create n nodes with a bunch of int properties
	for n := 0; n < numIntNodes; n++ {
		nums := make(map[string]int)
		x := n
		for i := 0; i < numIntProps; i++ {
			x = x + i*i
			nums[strconv.Itoa(i)] = x
		}
		_, err := sess.Run("CREATE (n:IntNode) SET n = $nums RETURN n", map[string]interface{}{"nums": nums})
		if err != nil {
			panic(err)
		}
	}

	sess.Run("CREATE (s:Setup {done: true}) RETURN s", nil)
}

func readNodes(driver neo4j.Driver) {
	sess, err := driver.NewSession(neo4j.SessionConfig{})
	if err != nil {
		panic(err)
	}
	defer sess.Close()

	result, err := sess.Run("MATCH (n:IntNode) RETURN n", nil)
	if err != nil {
		panic(err)
	}

	num := 0
	var record *neo4j.Record
	for result.NextRecord(&record) {
		num++
		node := record.Values[0].(neo4j.Node)
		if len(node.Props) != numIntProps {
			panic("Too few props")
		}
	}
	if num != numIntNodes {
		panic("Too few nodes")
	}
}

func readNodes18(driver neo4j18.Driver) {
	sess, err := driver.NewSession(neo4j18.SessionConfig{})
	if err != nil {
		panic(err)
	}
	defer sess.Close()

	result, err := sess.Run("MATCH (n:IntNode) RETURN n", nil)
	if err != nil {
		panic(err)
	}

	num := 0
	for result.Next() {
		num++
		node := result.Record().Values()[0].(neo4j18.Node)
		if len(node.Props()) != numIntProps {
			panic("Too few props")
		}
	}
	if num != numIntNodes {
		panic("Too few nodes")
	}
}

func main() {
	driver, err := neo4j.NewDriver(os.Args[1], neo4j.BasicAuth(os.Args[2], os.Args[3], ""), func(conf *neo4j.Config) {
		conf.Encrypted = false
		//conf.Log = neo4j.ConsoleLogger(neo4j.INFO)
	})
	if err != nil {
		panic(err)
	}

	// Build the setup if needed
	if !setupReady(driver) {
		buildSetup(driver)
	}

	readNodes(driver) // Warm up
	start := time.Now()
	readNodes(driver)
	duration := time.Since(start)

	driver18, err := neo4j18.NewDriver(os.Args[1], neo4j18.BasicAuth(os.Args[2], os.Args[3], ""), func(conf *neo4j18.Config) {
		conf.Encrypted = false
		//conf.Log = neo4j18.ConsoleLogger(neo4j.INFO)
	})
	readNodes18(driver18) // Warm up
	start18 := time.Now()
	readNodes18(driver18)
	duration18 := time.Since(start18)

	fmt.Printf("%s vs %s\n", duration, duration18)
}
