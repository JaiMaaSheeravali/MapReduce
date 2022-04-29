package main

//
// start a worker process, which is implemented
// in ../mr/worker.go. typically there will be
// multiple worker processes, talking to one coordinator.
//
// go run mrworker.go wc.so
//
// Please do not change this file.
//
import "mreduce/mr"
import "plugin"
import "log"
import "flag"

var (
	port  *string
	mrapp *string
)

func init() {
	port = flag.String("port", "1234", "port number")
	mrapp = flag.String("mrapp", "wc.so", "mrapp name : wc.so")
}

func main() {
	flag.Parse()

	mapf, reducef := loadPlugin(*mrapp)

	mr.MakeWorker(mapf, reducef, *port)
}

//
// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
//
func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []mr.KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
