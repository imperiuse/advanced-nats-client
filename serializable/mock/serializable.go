package mock

import (
	"errors"
)

type (
	DataMock    struct{ Data []byte }
	BadDataMock struct{}
)

var ErrBadDataMock = errors.New("err bad data mock")

func (d *DataMock) Marshal() ([]byte, error) {
	return d.Data, nil
}

func (d *DataMock) Unmarshal(data []byte) error {
	d.Data = data
	return nil
}

func (d *DataMock) Reset() {}

func (d *BadDataMock) Marshal() ([]byte, error) {
	return nil, ErrBadDataMock
}

func (d *BadDataMock) Unmarshal([]byte) error {
	return ErrBadDataMock
}

func (d *BadDataMock) Reset() {}
