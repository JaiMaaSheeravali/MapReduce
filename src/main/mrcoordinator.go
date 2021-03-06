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

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		os.Exit(1)
	}

	c := mr.MakeCoordinator(os.Args[1:], 2)
	for c.Done() == false {
		time.Sleep(5 * time.Second)
	}

	time.Sleep(time.Second)
}
