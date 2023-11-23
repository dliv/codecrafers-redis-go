package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

func main() {
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
			go handleConnection(conn)
		}
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	for {
		line := readLine(conn)
		fmt.Println("L: ", line)
		if len(line) < 1 {
			continue
		}
		err, resp := handleLine(conn, line)
		if err != nil {
			fmt.Println("Error for line: ", line, ", error: ", err.Error())
		} else {
			fmt.Println("Resp for line: ", line, ", resp: ", resp)
			conn.Write([]byte(resp))
		}
	}
}

func handleLine(conn net.Conn, line string) (error, string) {
	if strings.HasPrefix(line, "*") {
		sizeStr := line[1:]
		size, err := strconv.Atoi(sizeStr)
		if err != nil {
			return err, ""
		}
		return handleArray(conn, size)
	}
	return fmt.Errorf("Unknown line command '%s'", line), ""
}

func handleArray(conn net.Conn, size int) (error, string) {
	commandSizeLine := readLine(conn)
	fmt.Println("command size line: ", commandSizeLine)
	commandLine := strings.ToLower(readLine(conn))
	if commandLine == "ping" {
		return nil, "+PONG\r\n"
	}
	if commandLine == "echo" {
		echoStrSizeLine := readLine(conn)
		fmt.Println("echo string size line: ", echoStrSizeLine)
		echoArg := strings.ToLower(readLine(conn))
		return nil, "+" + echoArg + "\r\n"
	}
	return fmt.Errorf("Unknown line command '%s'", commandLine), ""
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
