package json

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var example = Example{
	Id:     1,
	Name:   "2",
	Price:  3,
	Status: ON,
	Result: []struct {
		Status bool              `json:"status,omitempty"`
		Msg    []byte            `json:"msg,omitempty"`
		Kv     map[string]string `json:"kv,omitempty"`
	}{{true, []byte{1, 2, 3}, map[string]string{"abc": "abc"}}},
}

func TestExample_Marshall(t *testing.T) {
	data, err := example.Marshal()
	assert.Nil(t, err)
	assert.NotNil(t, data)
}

func TestExample_Unmarshall(t *testing.T) {
	data, _ := example.Marshal()

	example2 := Example{}
	err := example2.Unmarshal(data)

	assert.Nil(t, err)
	assert.NotNil(t, example2)
	assert.Equal(t, example, example2)
}
