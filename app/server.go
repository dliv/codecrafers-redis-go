package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
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
	return val, ok
}

func (s *Storage) Set(key, val string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items[key] = StorageVal{
		payload: val,
		exp:     0,
	}
}

func main() {
	storage := NewStorage()

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
			go handleConnection(conn, storage)
		}
	}
}

func handleConnection(conn net.Conn, storage *Storage) {
	defer conn.Close()
	for {
		line := readLine(conn)
		fmt.Println("L: ", line)
		if len(line) < 1 {
			continue
		}
		err, resp := handleLine(storage, conn, line)
		if err != nil {
			fmt.Println("Error for line: ", line, ", error: ", err.Error())
		} else {
			fmt.Println("Resp for line: ", line, ", resp: ", resp)
			conn.Write([]byte(resp))
		}
	}
}

func handleLine(storage *Storage, conn net.Conn, line string) (error, string) {
	if strings.HasPrefix(line, "*") {
		sizeStr := line[1:]
		size, err := strconv.Atoi(sizeStr)
		if err != nil {
			return err, ""
		}
		return handleArray(storage, conn, size)
	}
	return fmt.Errorf("Unknown line command '%s'", line), ""
}

func handleArray(storage *Storage, conn net.Conn, size int) (error, string) {
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
		fmt.Println("key size line: ", keySizeLine)
		key := readLine(conn)
		valSizeLine := readLine(conn)
		fmt.Println("val size line: ", valSizeLine)
		val := readLine(conn)
		storage.Set(key, val)
		return nil, "+OK\r\n"
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
