package main

import (
	"fmt"
	"net"
	"os"
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
	line := ""
	buff := make([]byte, 1)
	for {
		n, err := conn.Read(buff)
		if err != nil {
			fmt.Println("Error reading: ", err.Error())
			break
		}
		if n < 1 {
			continue
		}
		ch := string(buff)
		fmt.Println("ch: ", ch)
		if buff[0] == '\n' || buff[0] == '\r' {
			if len(line) == 0 {
				continue
			}
			err, resp := handleLine(line)
			if err != nil {
				fmt.Println("Error for line: ", line, ", error: ", err.Error())
			} else {
				fmt.Println("Resp for line: ", line, ", resp: ", resp)
				conn.Write([]byte(resp))
			}
			line = ""
		} else {
			line += ch
		}
	}
}

func handleLine(line string) (error, string) {
	if line == "PING" || line == "ping" {
		return nil, "+PONG\r\n"
	}
	return fmt.Errorf("Unknown command '%s'", line), ""
}
