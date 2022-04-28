package main

//
// start the coordinator process, which is implemented
// in ../mr/coordinator.go
//
// go run mrcoordinator.go pg*.txt
//
// Please do not change this file.
//

import "mreduce/mr"
import "time"
import "os"
import "fmt"
import "github.com/colinmarc/hdfs/v2"
import "github.com/joho/godotenv"
import "log"
import "flag"
import "path"

var (
	port    *string
	nReduce *int
)

func init() {
	port = flag.String("port", "1234", "port number")
	nReduce = flag.Int("nreduce", 2, "number of reducers")
}

func goDotEnvVariable(key string) string {

	// load .env file
	err := godotenv.Load("../.env")
  
	if err != nil {
	  log.Fatalf("Error loading .env file")
	}
  
	return os.Getenv(key)
}

func main() {
	flag.Parse()

	if len(flag.Args()) == 0 {
		fmt.Fprintf(os.Stderr, "Usage: -port <port> -nreduce <number of reducer> mrcoordinator inputfiles...\n")
		os.Exit((1))
	}
	
	// connect to the hadoop file system
	hdfsClient, err := hdfs.New("localhost:9000")

	if err != nil {
		log.Fatal("Couldn't connect to HDFS\n", err)
	}

	c := mr.MakeCoordinator(flag.Args(), *nReduce, *port)
	fmt.Println("Listening on port: ", *port)
	for c.Done() == false {
		time.Sleep(5 * time.Second)
	}

	dir, err := hdfsClient.ReadDir(goDotEnvVariable("HDFS_TEMP_PATH"))
	for _, file := range dir {
		hdfsClient.RemoveAll(path.Join([]string{goDotEnvVariable("HDFS_TEMP_PATH"), file.Name()}...))
	}

	time.Sleep(time.Second)

}
