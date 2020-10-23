package bolt

import (
	"errors"
	"fmt"
	"time"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j/db"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/dbtype"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/internal/packstream"
)

// Generic hydrator that just stuffs supported primitives in a slice
type fieldsHydrator struct {
	tag    packstream.StructTag
	fields []interface{}
}

func (h *fieldsHydrator) GetHydrator(
	tag packstream.StructTag, num uint32) (packstream.Hydrator, error) {

	return nil, hydrationError
}

func (h *fieldsHydrator) reset(tag packstream.StructTag) {
	h.tag = tag
	h.fields = h.fields[:0]
}

func (h *fieldsHydrator) OnInt(i int64) {
	h.fields = append(h.fields, i)
}
func (h *fieldsHydrator) OnFloat(f float64) {
	h.fields = append(h.fields, f)
}
func (h *fieldsHydrator) OnString(s string) {
	h.fields = append(h.fields, s)
}
func (h *fieldsHydrator) OnBool(b bool)         {}
func (h *fieldsHydrator) OnBytes(b []byte)      {}
func (h *fieldsHydrator) OnNil()                {}
func (h *fieldsHydrator) OnArrayBegin(n uint32) {}
func (h *fieldsHydrator) OnArrayEnd()           {}
func (h *fieldsHydrator) OnMapBegin(n uint32)   {}
func (h *fieldsHydrator) OnMapEnd()             {}
func (h *fieldsHydrator) End() (interface{}, error) {
	switch h.tag {
	case msgIgnored:
		return hydrateIgnored(h.fields)
	case 'X':
		return hydratePoint2d(h.fields)
	case 'Y':
		return hydratePoint3d(h.fields)
	case 'F':
		return hydrateDateTimeOffset(h.fields)
	case 'f':
		return hydrateDateTimeNamedZone(h.fields)
	case 'd':
		return hydrateLocalDateTime(h.fields)
	case 'D':
		return hydrateDate(h.fields)
	case 'T':
		return hydrateTime(h.fields)
	case 't':
		return hydrateLocalTime(h.fields)
	case 'E':
		return hydrateDuration(h.fields)
	default:
		return nil, errors.New(fmt.Sprintf("Unknown tag: %02x", h.tag))
	}
}

func hydrateIgnored(fields []interface{}) (interface{}, error) {
	if len(fields) != 0 {
		return nil, errors.New("Ignored hydrate error")
	}
	return &ignoredResponse{}, nil
}

func hydrateFailure(fields []interface{}) (interface{}, error) {
	if len(fields) != 1 {
		return nil, errors.New("Failure hydrate error")
	}
	m, mok := fields[0].(map[string]interface{})
	if !mok {
		return nil, errors.New("Failure hydrate error")
	}
	code, cok := m["code"].(string)
	msg, mok := m["message"].(string)
	if !cok || !mok {
		return nil, errors.New("Failure hydrate error")
	}
	// Hydrate right into error defined in connection package to avoid remapping at a later
	// state.
	return &db.Neo4jError{Code: code, Msg: msg}, nil
}

func hydratePoint2d(fields []interface{}) (interface{}, error) {
	if len(fields) != 3 {
		return nil, errors.New("Point2d hydrate error")
	}
	srId, sok := fields[0].(int64)
	x, xok := fields[1].(float64)
	y, yok := fields[2].(float64)
	if !sok || !xok || !yok {
		return nil, errors.New("Point2d hydrate error")
	}
	return dbtype.Point2D{SpatialRefId: uint32(srId), X: x, Y: y}, nil
}

func hydratePoint3d(fields []interface{}) (interface{}, error) {
	if len(fields) != 4 {
		return nil, errors.New("Point3d hydrate error")
	}
	srId, sok := fields[0].(int64)
	x, xok := fields[1].(float64)
	y, yok := fields[2].(float64)
	z, zok := fields[3].(float64)
	if !sok || !xok || !yok || !zok {
		return nil, errors.New("Point3d hydrate error")
	}
	return dbtype.Point3D{SpatialRefId: uint32(srId), X: x, Y: y, Z: z}, nil
}

func hydrateDateTimeOffset(fields []interface{}) (interface{}, error) {
	if len(fields) != 3 {
		return nil, errors.New("DateTime hydrate error")
	}
	secs, sok := fields[0].(int64)
	nans, nok := fields[1].(int64)
	offs, ook := fields[2].(int64)
	if !sok || !nok || !ook {
		return nil, errors.New("DateTime hydrate error")
	}

	t := time.Unix(secs, nans).UTC()
	l := time.FixedZone("Offset", int(offs))
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), l), nil
}

func hydrateDateTimeNamedZone(fields []interface{}) (interface{}, error) {
	if len(fields) != 3 {
		return nil, errors.New("DateTime hydrate error")
	}
	secs, sok := fields[0].(int64)
	nans, nok := fields[1].(int64)
	zone, zok := fields[2].(string)
	if !sok || !nok || !zok {
		return nil, errors.New("DateTime hydrate error")
	}

	t := time.Unix(secs, nans).UTC()
	l, err := time.LoadLocation(zone)
	if err != nil {
		return nil, err
	}
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), l), nil
}

func hydrateLocalDateTime(fields []interface{}) (interface{}, error) {
	if len(fields) != 2 {
		return nil, errors.New("LocalDateTime hydrate error")
	}
	secs, sok := fields[0].(int64)
	nans, nok := fields[1].(int64)
	if !sok || !nok {
		return nil, errors.New("LocalDateTime hydrate error")
	}
	t := time.Unix(secs, nans).UTC()
	t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.Local)
	return dbtype.LocalDateTime(t), nil
}

func hydrateDate(fields []interface{}) (interface{}, error) {
	if len(fields) != 1 {
		return nil, errors.New("Date hydrate error")
	}
	days, dok := fields[0].(int64)
	if !dok {
		return nil, errors.New("Date hydrate error")
	}
	secs := days * 86400
	return dbtype.Date(time.Unix(secs, 0).UTC()), nil
}

func hydrateTime(fields []interface{}) (interface{}, error) {
	if len(fields) != 2 {
		return nil, errors.New("Time hydrate error")
	}
	nans, nok := fields[0].(int64)
	offs, ook := fields[1].(int64)
	if !nok || !ook {
		return nil, errors.New("Time hydrate error")
	}
	secs := nans / int64(time.Second)
	nans -= secs * int64(time.Second)
	l := time.FixedZone("Offset", int(offs))
	t := time.Date(0, 0, 0, 0, 0, int(secs), int(nans), l)
	return dbtype.Time(t), nil
}

func hydrateLocalTime(fields []interface{}) (interface{}, error) {
	if len(fields) != 1 {
		return nil, errors.New("LocalTime hydrate error")
	}
	nans, nok := fields[0].(int64)
	if !nok {
		return nil, errors.New("LocalTime hydrate error")
	}
	secs := nans / int64(time.Second)
	nans -= secs * int64(time.Second)
	t := time.Date(0, 0, 0, 0, 0, int(secs), int(nans), time.Local)
	return dbtype.LocalTime(t), nil
}

func hydrateDuration(fields []interface{}) (interface{}, error) {
	// Always hydrate to dbtype.Duration since that allows for longer durations than the
	// standard time.Duration even though it's probably very unusual with the need to
	// express durations for hundreds of years.
	if len(fields) != 4 {
		return nil, errors.New("Duration hydrate error")
	}
	mon, mok := fields[0].(int64)
	day, dok := fields[1].(int64)
	sec, sok := fields[2].(int64)
	nan, nok := fields[3].(int64)
	if !mok || !dok || !sok || !nok {
		return nil, errors.New("Duration hydrate error")
	}

	return dbtype.Duration{Months: mon, Days: day, Seconds: sec, Nanos: int(nan)}, nil
}
