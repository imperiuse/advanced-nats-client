package uuid

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUuidS_UUID(t *testing.T) {
	uid := MustUUID4()
	assert.NotNil(t, uid)
	assert.Equal(t, 36, len(uid))

	uid = UUID4()
	assert.NotNil(t, uid)
	assert.Equal(t, 36, len(uid))
}
