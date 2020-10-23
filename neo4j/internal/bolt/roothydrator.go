package bolt

import (
	"errors"
	"fmt"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j/internal/packstream"
)

var hydrationError error = errors.New("Hydration error")

type rootHydrator struct {
	success successHydrator
	failure failureHydrator
	fields  fieldsHydrator
}

func (h *rootHydrator) GetHydrator(
	tag packstream.StructTag, num uint32) (packstream.Hydrator, error) {

	switch tag {
	case msgSuccess:
		h.success.reset()
		return &h.success, nil
	case msgFailure:
		h.failure.reset()
		return &h.failure, nil
	case 'X', 'Y', 'F', 'f', 'd', 'D', 'T', 't', 'E':
		h.fields.reset(tag)
		return &h.fields, nil
	}
	return nil, errors.New(fmt.Sprintf("Unknown tag:%d", tag))
}

func (h *rootHydrator) OnInt(i int64)         {}
func (h *rootHydrator) OnBool(b bool)         {}
func (h *rootHydrator) OnFloat(f float64)     {}
func (h *rootHydrator) OnString(s string)     {}
func (h *rootHydrator) OnBytes(b []byte)      {}
func (h *rootHydrator) OnNil()                {}
func (h *rootHydrator) OnArrayBegin(n uint32) {}
func (h *rootHydrator) OnArrayEnd()           {}
func (h *rootHydrator) OnMapBegin(n uint32)   {}
func (h *rootHydrator) OnMapEnd()             {}
func (h *rootHydrator) End() (interface{}, error) {
	return nil, nil
}
