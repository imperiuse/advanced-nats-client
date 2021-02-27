package uuid

import (
	"fmt"

	"go.uber.org/zap"

	"github.com/gofrs/uuid"

	"github.com/imperiuse/advance-nats-client/logger"
)

type (
	// UUID = uuid.UUID.
	UUID = uuid.UUID
)

// EmptyStringUUID = all zero.
var EmptyStringUUID = "00000000-0000-0000-0000-000000000000"

// MustUUID4 - YES it's panic, use in init or example case only! Please be carefully!
func MustUUID4() string {
	uid, err := uuid.NewV4()
	if err != nil {
		panic(err)
	}

	return fmt.Sprint(uid)
}

// UUID4 - return string presentation of UUID ver4 use uuid.NewV4() under the hood.
func UUID4() string {
	uid, err := uuid.NewV4()
	if err != nil {
		logger.Log.Error("UUID generate error", zap.Error(err))

		return EmptyStringUUID
	}

	return fmt.Sprint(uid)
}
