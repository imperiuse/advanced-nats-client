package uuid

import (
	"github.com/gofrs/uuid"
	"go.uber.org/zap"

	"github.com/imperiuse/advanced-nats-client/v1/logger"
)

type (
	// UUID is an alias for uuid.UUID.
	UUID = uuid.UUID
)

// EmptyStringUUID is a UUID string with all zeroes.
var (
	EmptyStringUUID  = "00000000-0000-0000-0000-000000000000"
	EmptyStringUUIDb = [16]byte{
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	}
)

// MustUUID4 generates a UUID v4. It panics in case of an error. Use only in initialization or example cases.
// Please be cautious with this function!
func MustUUID4() string {
	uid, err := uuid.NewV4()
	if err != nil {
		panic(err)
	}

	return uid.String()
}

// UUID4 returns a string representation of a UUID v4 using uuid.NewV4() under the hood.
func UUID4() string {
	uid, err := uuid.NewV4()
	if err != nil {
		logger.Log.Error("UUID v4 generation error", zap.Error(err))

		return EmptyStringUUID
	}

	return uid.String()
}

// UUID4b returns a [16]byte representation of a UUID v4 using uuid.NewV4() under the hood.
func UUID4b() [16]byte {
	uid, err := uuid.NewV4()
	if err != nil {
		logger.Log.Error("UUID v4 generation error", zap.Error(err))

		return EmptyStringUUIDb
	}

	return uid
}
