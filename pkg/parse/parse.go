package parse

import (
	"fmt"
	"os"
)

const DB_SELECT = 0xFE
const RESIZE = 0xFB
const EOF = 0xFF
const EXP_S = 0xFD
const EXP_M = 0xFC

func ParseRedisDb(dbPath string, dbNum int) (map[string]string, error) {
	// lock ? someone could write while we're reading - append only?
	file, err := os.Open(dbPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	idx := 0
	buffer := make([]byte, 1)
	at_select := false
	for {
		n, err := file.Read(buffer)
		if err != nil {
			return nil, err
		}
		if n > 0 {
			debug_byte("", buffer[0])
			if at_select {
				at_select = false
				if buffer[0] == byte(dbNum) {
					fmt.Println("SELECTED DB: ", dbNum, " found at idx: ", idx)
					return parseKeys(file)
				}
			} else if buffer[0] == DB_SELECT {
				at_select = true
			}
		} else {
			break
		}
		idx += 1
	}
	return nil, fmt.Errorf("failed to find DB: %d", dbNum)
}

func debug_byte(prefix string, b byte) {
	char := rune(b)
	if char < 32 || char > 126 {
		char = '.'
	}
	fmt.Printf("%s%c %3d %02x %08b\n", prefix, char, b, b, b)
}

func parseKeys(file *os.File) (map[string]string, error) {
	buffer := make([]byte, 1)
	n, err := file.Read(buffer)
	if n < 1 {
		return nil, fmt.Errorf("failed to read key type")
	}
	if err != nil {
		return nil, err
	}

	firstByte := buffer[0]
	debug_byte("\t", firstByte)
	if firstByte == RESIZE {
		fmt.Println("RESIZED")
		return parseKeys(file)
	}

	// size of hash table
	if firstByte != byte(1) {
		return nil, fmt.Errorf("expected single entry hash table for this step")
	}

	// skip "size of expires hash table"
	n, err = file.Read(buffer)
	if n < 1 || err != nil {
		return nil, fmt.Errorf("failed to read size of expires hash table")
	}

	// what is this byte?
	n, err = file.Read(buffer)
	if n < 1 || err != nil {
		return nil, fmt.Errorf("failed to read wtf byte")
	}

	parsed := make(map[string]string)

	n, err = file.Read(buffer)
	if n < 1 || err != nil {
		return nil, fmt.Errorf("failed to read key length byte")
	}

	if Left_2_bits(buffer[0]) != byte(0) {
		return nil, fmt.Errorf("expected 0b00 for first two bits of key length")
	}

	keyLength := Right_6_bits(buffer[0])
	key := ""
	for i := 0; i < int(keyLength); i++ {
		n, err = file.Read(buffer)
		if n < 1 || err != nil {
			return nil, fmt.Errorf("failed to read part of key")
		}
		key += string(buffer[0])
	}

	n, err = file.Read(buffer)
	if n < 1 || err != nil {
		return nil, fmt.Errorf("failed to read val length byte")
	}

	if Left_2_bits(buffer[0]) != byte(0) {
		return nil, fmt.Errorf("expected 0b00 for first two bits of value length")
	}

	valLength := Right_6_bits(buffer[0])
	val := ""
	for i := 0; i < int(valLength); i++ {
		n, err = file.Read(buffer)
		if n < 1 || err != nil {
			return nil, fmt.Errorf("failed to read part of key")
		}
		val += string(buffer[0])
	}

	parsed[key] = val

	// if firstByte == EXP_M || firstByte == EXP_S {
	// 	return nil, fmt.Errorf("expiring keys not supported")
	// }

	// if Left_2_bits(firstByte) != byte(3) {
	// 	return nil, fmt.Errorf("expected 0b11 for first two bits")
	// }

	// format := Right_6_bits(firstByte)
	// if format == byte(0) {
	// 	return nil, fmt.Errorf("format 8-bit int not supported yet")
	// } else if format == byte(1) {
	// 	return nil, fmt.Errorf("format 16-bit int not supported yet")
	// } else if format == byte(2) {
	// 	return nil, fmt.Errorf("format 32-bit int not supported yet")
	// } else if format == byte(3) {
	// 	return nil, fmt.Errorf("format LZF not supported yet")
	// }

	// for {
	// 	n, err := file.Read(buffer)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	if n < 1 {
	// 		return parsed, nil
	// 	}
	// 	b := buffer[0]
	// 	debug_byte("\t", b)
	// 	if b == DB_SELECT || b == EOF {
	// 		return parsed, nil
	// 	}
	// }

	return parsed, nil
}

func Left_2_bits(b byte) byte {
	return b & 0b11_00_00_00 >> 6
}

func Right_6_bits(b byte) byte {
	return b & 0b00_11_11_11
}
