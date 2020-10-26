package packstream

import (
	"fmt"
	"reflect"
)

type OverflowError struct {
	msg string
}

func (e *OverflowError) Error() string {
	return e.msg
}

type UnsupportedTypeError struct {
	t reflect.Type
}

func (e *UnsupportedTypeError) Error() string {
	return fmt.Sprintf("Packing of type '%s' is not supported", e.t.String())
}

type IoError struct{}

func (e *IoError) Error() string {
	return "IO error"
}

type IllegalFormatError struct {
	msg string
}

func (e *IllegalFormatError) Error() string {
	return e.msg
}

type UnpackError struct {
	msg string
}

func (e *UnpackError) Error() string {
	return e.msg
}
