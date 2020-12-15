package mock

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_StupidMarshal(t *testing.T) {
	data := []byte("abs")
	d := DataMock{Data: data}
	dataM, err := d.Marshal()
	assert.Nil(t, err)
	assert.Equal(t, dataM, data)

	b := BadDataMock{}
	_, err = b.Marshal()
	assert.Equal(t, ErrBadDataMock, err)
}

func Test_StupidUnmarshal(t *testing.T) {
	d := DataMock{}
	data := []byte("abs")
	err := d.Unmarshal(data)
	assert.Nil(t, err)
	assert.Equal(t, d.Data, data)

	b := BadDataMock{}
	err = b.Unmarshal(data)
	assert.Equal(t, ErrBadDataMock, err)
}
