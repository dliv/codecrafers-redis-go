package parse

import (
	"codecrafters-redis-go/pkg/storage"
	"encoding/binary"
	"fmt"
	"os"
)

const DB_SELECT = 0xFE
const RESIZE = 0xFB
const EOF = 0xFF
const EXP_S = 0xFD
const EXP_M = 0xFC

func DebugPrintRedisDb(dbPath, prefix string) error {
	file, err := os.Open(dbPath)
	if err != nil {
		return err
	}
	defer file.Close()

	buffer := make([]byte, 1)
	for {
		n, err := file.Read(buffer)
		if err != nil {
			return err
		}
		if n > 0 {
			debug_byte(prefix, buffer[0])
		} else {
			return nil
		}
	}
}

func ParseRedisDb(dbPath string, dbNum int, nowUnixMs int64) (map[string]storage.StorageVal, error) {
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
					return parseKeys(file, nowUnixMs)
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

func parseKeys(file *os.File, nowUnixMs int64) (map[string]storage.StorageVal, error) {
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
		return parseKeys(file, nowUnixMs)
	}

	entriesCount := int(firstByte)

	fmt.Println("entriesCount: ", entriesCount)

	// skip "size of expires hash table"
	n, err = file.Read(buffer)
	if n < 1 || err != nil {
		return nil, fmt.Errorf("failed to read size of expires hash table")
	}

	parsed := make(map[string]storage.StorageVal)

	for i := 0; i < entriesCount; i++ {
		key, val, err := readEntry(file)
		if err != nil {
			fmt.Println("error in entry: ", i)
			return parsed, err
		}
		if val.Exp == int64(0) || nowUnixMs <= val.Exp {
			fmt.Println("key ", key, " not expired, Exp: ", val.Exp, ", now: ", nowUnixMs)
			parsed[key] = val
		} else {
			fmt.Println("EXPIRED key: ", key, ", Exp: ", val.Exp, ", now: ", nowUnixMs)
		}
	}

	return parsed, nil
}

func readEntry(file *os.File) (string, storage.StorageVal, error) {
	buffer := make([]byte, 1)

	// TODO: parse out seconds expiration if applicable
	exp := int64(0)

	n, err := file.Read(buffer)
	firstByte := buffer[0]
	if n < 1 || err != nil {
		return "", storage.StorageVal{}, fmt.Errorf("failed to read entry's first byte")
	}

	if firstByte == EXP_M {
		exp, err = readExpMillis(file)
		if err != nil {
			return "", storage.StorageVal{}, err
		}

		// not sure what this extra byte is
		n, err := file.Read(buffer)
		if n < 1 || err != nil {
			return "", storage.StorageVal{}, fmt.Errorf("failed to read extra byte after exp millis")
		}
	}

	if firstByte == EXP_S {
		return "", storage.StorageVal{}, fmt.Errorf("expiring keys with seconds not supported")
	}

	n, err = file.Read(buffer)
	if n < 1 || err != nil {
		return "", storage.StorageVal{}, fmt.Errorf("failed to read key length byte")
	}

	if Left_2_bits(buffer[0]) != byte(0) {
		return "", storage.StorageVal{}, fmt.Errorf("expected 0b00 for first two bits of key length")
	}

	keyLength := Right_6_bits(buffer[0])
	key := ""
	for i := 0; i < int(keyLength); i++ {
		n, err = file.Read(buffer)
		if n < 1 || err != nil {
			return "", storage.StorageVal{}, fmt.Errorf("failed to read part of key")
		}
		key += string(buffer[0])
	}

	n, err = file.Read(buffer)
	if n < 1 || err != nil {
		return "", storage.StorageVal{}, fmt.Errorf("failed to read val length byte")
	}

	if Left_2_bits(buffer[0]) != byte(0) {
		return "", storage.StorageVal{}, fmt.Errorf("expected 0b00 for first two bits of value length")
	}

	valLength := Right_6_bits(buffer[0])
	val := ""
	for i := 0; i < int(valLength); i++ {
		n, err = file.Read(buffer)
		if n < 1 || err != nil {
			return "", storage.StorageVal{}, fmt.Errorf("failed to read part of key")
		}
		val += string(buffer[0])
	}

	sv := storage.StorageVal{
		Payload: val,
		Exp:     exp,
	}

	return key, sv, nil
}

func readExpMillis(file *os.File) (int64, error) {
	buffer := make([]byte, 8)
	n, err := file.Read(buffer)
	if n < 8 || err != nil {
		return int64(0), fmt.Errorf("failed to read exp millis byte")
	}
	exp := binary.LittleEndian.Uint64(buffer)
	scaled := exp // / 1_000
	return int64(scaled), nil
}

func Left_2_bits(b byte) byte {
	return b >> 6
}

func Right_6_bits(b byte) byte {
	return b & 0b00_11_11_11
}
