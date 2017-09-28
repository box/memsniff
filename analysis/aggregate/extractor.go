package aggregate

import (
	"bytes"
	"github.com/box/memsniff/protocol/model"
	"regexp"
	"strconv"
)

var aggregatorRegex *regexp.Regexp

func init() {
	var err error
	aggregatorRegex, err = regexp.Compile(`^([a-z0-9]+)\(([a-z]+)\)$`)
	if err != nil {
		panic(err)
	}
}

func Size(e model.Event) int64 {
	return int64(e.Size)
}

func fieldIdFromDescriptor(desc string) (model.EventFieldMask, error) {
	switch desc {
	case "key":
		return model.FieldKey, nil
	case "size":
		return model.FieldSize, nil
	default:
		return 0, BadDescriptorError(desc)
	}
}

func fieldsAsString(e model.Event, mask model.EventFieldMask) string {
	var buf bytes.Buffer
	for id := model.EventFieldMask(1); id < model.FieldEndOfFields; id <<= 1 {
		if mask&id == id {
			buf.WriteString(fieldAsString(e, id))
			buf.WriteByte(0)
		}
	}
	return buf.String()
}

func fieldAsString(e model.Event, id model.EventFieldMask) string {
	switch id {
	case model.FieldKey:
		return e.Key
	case model.FieldSize:
		return strconv.Itoa(e.Size)
	default:
		panic("bad fieldId")
	}
}

func fieldAsInt64(e model.Event, id model.EventFieldMask) int64 {
	switch id {
	case model.FieldSize:
		return int64(e.Size)
	default:
		panic("bad fieldId")
	}
}
