package kafkaadapt

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"reflect"
	"strconv"
	"strings"
)

const jsonPathDelimiter = "."

func LoadJsonConfig(fileName string) (Config, error) {
	cfg := &jsonConfig{
		cfg: make(map[string]interface{}),
	}
	data, err := ioutil.ReadFile(fileName)
	if err != nil {
		return nil, fmt.Errorf("cant read config: %v", err)
	}

	err = json.Unmarshal(data, &cfg.cfg)
	if err != nil {
		return nil, fmt.Errorf("cant unmarshal config: %v", err)
	}

	return cfg, nil
}

type jsonConfig struct {
	cfg map[string]interface{}
}

func (j *jsonConfig) getValByPath(path string) (interface{}, error) {
	names := strings.Split(path, jsonPathDelimiter)
	var v interface{} = j.cfg
	for _, name := range names {
		switch m := v.(type) {
		case map[string]interface{}:
			v = m[name]
		default:
			return nil, fmt.Errorf("cant get value for %v, element %v doesn't exist", path, name)
		}
	}
	return v, nil
}

func (j *jsonConfig) GetInt(path string) (int, error) {
	val, err := j.getValByPath(path)
	if err != nil {
		return 0, err
	}

	if res, ok := val.(int); ok {
		return res, nil
	}
	switch k := reflect.ValueOf(val).Kind(); k {
	case reflect.Float32:
		return int(val.(float32)), nil
	case reflect.Float64:
		return int(val.(float64)), nil
	case reflect.String:
		if val, err := strconv.Atoi(val.(string)); err == nil {
			return val, nil
		}
	}
	return 0, fmt.Errorf("value of %v is %v type and has unconvertable value %v", path, reflect.TypeOf(val), val)
}

func (j *jsonConfig) GetString(path string) (string, error) {
	val, err := j.getValByPath(path)
	if err != nil {
		return "", err
	}
	switch t := val.(type) {
	case string:
		return val.(string), nil
	default:
		return "", fmt.Errorf("value %v is %v type", path, t)
	}
}
