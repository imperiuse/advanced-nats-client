package json

import (
	"encoding/json"
)

const (
	UNKNOWN = iota
	ON
	OFF
)

// This Example struct  is as close as possible to a protobuf struct ./examples/protocols/protobuf/Example
type Example struct {
	Id     int64   `json:"id,omitempty"`
	Name   string  `json:"name,omitempty"`
	Price  float64 `json:"price,omitempty"`
	Status int     // the Go language is more poorer than protobuf lang, it doesn't have Enum :(
	Result []struct {
		Status bool              `json:"status,omitempty"`
		Msg    []byte            `json:"msg,omitempty"`
		Kv     map[string]string `json:"kv,omitempty"`
	} `json:"result,omitempty"`
}

func (e *Example) Marshal() ([]byte, error) {
	return json.Marshal(e)
}

func (e *Example) Unmarshal(data []byte) error {
	var ex Example
	err := json.Unmarshal(data, &ex)
	if err == nil {
		*e = ex
	}
	return err
}

func (e *Example) Reset() {}
