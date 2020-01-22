package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	_ "bazil.org/fuse/fs/fstestutil" // needed if fuse.debug is used
)

var (
	fileName string
)

func usage() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s MOUNTPOINT\n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	flag.Usage = usage
	flag.Parse()

	if flag.NArg() < 1 {
		usage()
		os.Exit(2)
	}
	mountpoint := flag.Arg(0)

	c, err := fuse.Mount(
		mountpoint,
		fuse.FSName("rsfs"),
		fuse.Subtype("streamfs"),
		fuse.LocalVolume(),
		fuse.VolumeName("Redis Streams"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	rClient, err := newRedisClient([]string{"127.0.0.1:6379"})
	if err != nil {
		log.Fatal("failed to connect to redis: %s", err.Error())
	}

	go server()

	err = fs.Serve(c, &redisFS{
		client:       rClient,
		attrValidity: 1 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}

	// check if the mount process has an error to report
	<-c.Ready
	if err := c.MountError; err != nil {
		log.Fatal(err)
	}
}

func server() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fileName = r.URL.Path[1:]
		w.WriteHeader(http.StatusOK)
	})

	log.Fatal(http.ListenAndServe(":8888", nil))
}
