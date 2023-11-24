package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type StorageVal struct {
	payload string
	exp     int64
}

type Storage struct {
	mu    sync.RWMutex
	items map[string]StorageVal
}

func NewStorage() *Storage {
	return &Storage{
		items: make(map[string]StorageVal),
	}
}

func (s *Storage) Get(key string) (StorageVal, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, ok := s.items[key]
	fmt.Println("Get key: ", key, ", val: ", val.payload, ", exp: ", val.exp, ", ok: ", ok)
	if !ok {
		fmt.Println("missing")
		return StorageVal{}, false
	}
	if val.exp == int64(0) {
		fmt.Println("no exp")
		return val, true
	}
	now := time.Now().UnixMilli()
	fmt.Println("now: ", now)
	if now > val.exp {
		fmt.Println("expired")
		delete(s.items, key)
		return StorageVal{}, false
	}
	fmt.Println("current")
	return val, true
}

func (s *Storage) Set(key, payload string, expiresIn int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	exp := int64(0)
	now := time.Now().UnixMilli()
	fmt.Println("expiresIn: ", expiresIn)
	fmt.Println("now: ", now)
	if expiresIn > int64(0) {
		exp = now + expiresIn
	}
	fmt.Println("Set key: ", key, ", payload: ", payload, ", exp: ", exp)
	s.items[key] = StorageVal{
		payload,
		exp,
	}
}

func (s *Storage) Del(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.items, key)
}

type DbFile struct {
	dir      string
	filename string
}

func main() {
	storage := NewStorage()

	dbFile := DbFile{
		dir:      ".",
		filename: "dump.rdb",
	}

	for i, arg := range os.Args {
		if arg == "--dir" || arg == "-d" {
			dbFile.dir = os.Args[i+1]
		} else if arg == "--dbfilename" || arg == "-f" {
			dbFile.filename = os.Args[i+1]
		}
	}

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
		} else {
			go handleConnection(conn, storage, dbFile)
		}
	}
}

func handleConnection(conn net.Conn, storage *Storage, dbFile DbFile) {
	defer conn.Close()
	for {
		line := readLine(conn)
		fmt.Println("L: ", line)
		if len(line) < 1 {
			continue
		}
		err, resp := handleLine(storage, dbFile, conn, line)
		if err != nil {
			fmt.Println("Error for line: ", line, ", error: ", err.Error())
		} else {
			fmt.Println("Resp for line: ", line, ", resp: ", resp)
			conn.Write([]byte(resp))
		}
	}
}

func handleLine(storage *Storage, dbFile DbFile, conn net.Conn, line string) (error, string) {
	if strings.HasPrefix(line, "*") {
		sizeStr := line[1:]
		size, err := strconv.Atoi(sizeStr)
		if err != nil {
			return err, ""
		}
		return handleArray(storage, dbFile, conn, size)
	}
	return fmt.Errorf("Unknown line command '%s'", line), ""
}

func handleArray(storage *Storage, dbFile DbFile, conn net.Conn, size int) (error, string) {
	fmt.Println("handleArray size: ", size)
	commandSizeLine := readLine(conn)
	fmt.Println("command size line: ", commandSizeLine)
	command := strings.ToLower(readLine(conn))
	if command == "ping" {
		return nil, "+PONG\r\n"
	}
	if command == "echo" {
		echoStrSizeLine := readLine(conn)
		fmt.Println("echo string size line: ", echoStrSizeLine)
		echoArg := strings.ToLower(readLine(conn))
		return nil, "+" + echoArg + "\r\n"
	}
	if command == "set" {
		keySizeLine := readLine(conn)
		fmt.Println("key size: ", keySizeLine)
		key := readLine(conn)
		fmt.Println("key: ", key)
		valSizeLine := readLine(conn)
		fmt.Println("val size: ", valSizeLine)
		val := readLine(conn)
		fmt.Println("val: ", val)
		expiresIn := int64(0)
		if size == 5 {
			subcommandSize := readLine(conn)
			fmt.Println("subcommand size: ", subcommandSize)
			subcommand := readLine(conn)
			fmt.Println("subcommand size: ", subcommand)
			subcommandValSize := readLine(conn)
			fmt.Println("subcommandVal size: ", subcommandValSize)
			subcommandVal := readLine(conn)
			fmt.Println("subcommandVal: ", subcommandVal)
			if subcommand == "px" || subcommand == "PX" {
				expiresInRaw, err := strconv.Atoi(subcommandVal)
				if err != nil {
					return err, ""
				}
				expiresIn = int64(expiresInRaw)
			}
		}
		storage.Set(key, val, expiresIn)
		return nil, "+OK\r\n"
	}
	if command == "config" {
		getOrSetSize := readLine(conn)
		fmt.Println("getOrSet size: ", getOrSetSize)
		getOrSet := readLine(conn)
		fmt.Println("getOrSet: ", getOrSet)
		keySize := readLine(conn)
		fmt.Println("key size: ", keySize)
		key := readLine(conn)
		fmt.Println("key: ", key)
		if getOrSet == "get" {
			if key == "dbfilename" {
				return nil, "*2\r\n$10\r\ndbfilename\r\n$" + (strconv.Itoa(len(dbFile.filename))) + "\r\n" + dbFile.filename + "\r\n"
			}
			if key == "dir" {
				return nil, "*2\r\n$3\r\ndir\r\n$" + (strconv.Itoa(len(dbFile.dir))) + "\r\n" + dbFile.dir + "\r\n"
			}
			return fmt.Errorf("Unknown config key '%s'", key), ""
		}
		return fmt.Errorf("Unknown config subcommand '%s'", getOrSet), ""
	}
	if command == "get" {
		keySizeLine := readLine(conn)
		fmt.Println("key size line: ", keySizeLine)
		key := readLine(conn)
		val, ok := storage.Get(key)
		if !ok {
			return nil, "$-1\r\n"
		}
		return nil, "$" + strconv.Itoa(len(val.payload)) + "\r\n" + val.payload + "\r\n"
	}
	return fmt.Errorf("Unknown line command '%s'", command), ""
}

func readLine(conn net.Conn) string {
	line := ""
	buff := make([]byte, 1)
	for {
		n, err := conn.Read(buff)
		if err != nil {
			continue
		}
		if n < 1 {
			continue
		}
		ch := string(buff)
		if buff[0] == '\r' {
			n, err := conn.Read(buff)
			if err != nil {
				return ""
			}
			if n < 1 {
				continue
			}
			if buff[0] == '\n' {
				if len(line) == 0 {
					continue
				}
				return line
			} else {
				line += ch
				line += string(buff[0])
			}
		} else {
			line += ch
		}
	}
}
