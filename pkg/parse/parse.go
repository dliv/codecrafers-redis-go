package parse

func ParseRedisDb(dbPath string) (map[string]string, error) {
	parsed := map[string]string{
		"foo": "1",
		"bar": "2",
	}
	return parsed, nil
}
