package main

import (
	"codecrafters-redis-go/pkg/parse"
	"codecrafters-redis-go/pkg/storage"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Args struct {
	dir      string
	filename string
	dbNum    int
}

func GetArgs() Args {
	args := Args{
		filename: "dump.rdb",
	}
	for i, arg := range os.Args {
		if arg == "--dir" || arg == "-d" {
			args.dir = os.Args[i+1]
		} else if arg == "--dbfilename" || arg == "-f" {
			args.filename = os.Args[i+1]
		} else if arg == "-n" {
			dbNum, err := strconv.Atoi(os.Args[i+1])
			if err != nil {
				fmt.Println("Error parsing db number: ", err.Error())
				os.Exit(1)
			}
			args.dbNum = dbNum
		}
	}
	return args
}

func (args Args) GetDumpFileRaw() string {
	dir := args.dir
	if dir == "" {
		dir = "./"
	} else if !strings.HasSuffix(dir, "/") {
		dir += "/"
	}
	path := dir + args.filename
	return path
}

func (args Args) GetDumpFile(now int64) (map[string]storage.StorageVal, error) {
	path := args.GetDumpFileRaw()
	return parse.ParseRedisDb(path, args.dbNum, now)
}
