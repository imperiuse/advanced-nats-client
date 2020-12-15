package serializable

type (
	Serializable interface {
		Marshal() ([]byte, error)
		Unmarshal([]byte) error
	}
)
