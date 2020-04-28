package timequeue

import (
	"encoding"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"reflect"
)

type binaryEncoder struct {
	Order  binary.ByteOrder
	w      io.Writer
	buf    []byte
	strict bool
}

func newBinaryEncoder(w io.Writer) *binaryEncoder {
	return &binaryEncoder{
		Order: binary.LittleEndian,
		w:     w,
		buf:   make([]byte, 8),
	}
}

// newStrictEncoder creates an binaryEncoder similar to newEncoder, however
// if this binaryEncoder attempts to encode a struct and the struct has no encodable
// fields an error is returned whereas the binaryEncoder returned from newBinaryEncoder
// will simply not write anything to `w`.
func newStrictBinaryEncoder(w io.Writer) *binaryEncoder {
	enc := newBinaryEncoder(w)
	enc.strict = true
	return enc
}

func (enc *binaryEncoder) writeVarint(v int) error {
	l := binary.PutUvarint(enc.buf, uint64(v))
	_, err := enc.w.Write(enc.buf[:l])
	return err
}

func (enc *binaryEncoder) encode(v interface{}) (err error) {
	switch cv := v.(type) {
	case encoding.BinaryMarshaler:
		buf, err := cv.MarshalBinary()
		if err != nil {
			return err
		}
		if err = enc.writeVarint(len(buf)); err != nil {
			return err
		}
		_, err = enc.w.Write(buf)

	case []byte: // fast-path byte arrays
		if err = enc.writeVarint(len(cv)); err != nil {
			return
		}
		_, err = enc.w.Write(cv)

	default:
		rv := reflect.Indirect(reflect.ValueOf(v))
		t := rv.Type()

		switch t.Kind() {
		case reflect.Array:
			l := t.Len()
			for i := 0; i < l; i++ {
				if err = enc.encode(rv.Index(i).Addr().Interface()); err != nil {
					return
				}
			}

		case reflect.Slice:
			l := rv.Len()
			if err = enc.writeVarint(l); err != nil {
				return
			}
			for i := 0; i < l; i++ {
				if err = enc.encode(rv.Index(i).Addr().Interface()); err != nil {
					return
				}
			}

		case reflect.Struct:
			l := rv.NumField()
			n := 0
			for i := 0; i < l; i++ {
				if v := rv.Field(i); t.Field(i).Name != "_" {
					if err = enc.encode(v.Interface()); err != nil {
						return
					}
					n++
				}
			}
			if enc.strict && n == 0 {
				return fmt.Errorf("binary: struct had no encodable fields")
			}

		case reflect.Map:
			l := rv.Len()
			if err = enc.writeVarint(l); err != nil {
				return
			}
			for _, key := range rv.MapKeys() {
				value := rv.MapIndex(key)
				if err = enc.encode(key.Interface()); err != nil {
					return err
				}
				if err = enc.encode(value.Interface()); err != nil {
					return err
				}
			}

		case reflect.String:
			if err = enc.writeVarint(rv.Len()); err != nil {
				return
			}
			_, err = enc.w.Write([]byte(rv.String()))

		case reflect.Bool:
			var out byte
			if rv.Bool() {
				out = 1
			}
			err = binary.Write(enc.w, enc.Order, out)

		case reflect.Int:
			err = binary.Write(enc.w, enc.Order, int64(rv.Int()))

		case reflect.Uint:
			err = binary.Write(enc.w, enc.Order, int64(rv.Uint()))

		case reflect.Int8, reflect.Uint8, reflect.Int16, reflect.Uint16,
			reflect.Int32, reflect.Uint32, reflect.Int64, reflect.Uint64,
			reflect.Float32, reflect.Float64,
			reflect.Complex64, reflect.Complex128:
			err = binary.Write(enc.w, enc.Order, v)

		default:
			return errors.New("binary: unsupported type " + t.String())
		}
	}
	return
}

type byteReader struct {
	io.Reader
}

func (b *byteReader) ReadByte() (byte, error) {
	var buf [1]byte
	if _, err := io.ReadFull(b, buf[:]); err != nil {
		return 0, err
	}
	return buf[0], nil
}

type binaryDecoder struct {
	Order       binary.ByteOrder
	r           *byteReader
	decodeValue bool
	value       interface{}
	registry    map[string]interface{}
}

func newBinaryDecoder(r io.Reader, value interface{}, registry map[string]interface{}) *binaryDecoder {
	return &binaryDecoder{
		Order:    binary.LittleEndian,
		r:        &byteReader{r},
		value:    value,
		registry: registry,
	}
}

func (d *binaryDecoder) decode(v interface{}) (err error) {
	// Check if the type implements the encoding.BinaryUnmarshaler interface, and use it if so.
	if i, ok := v.(encoding.BinaryUnmarshaler); ok {
		var l uint64
		if l, err = binary.ReadUvarint(d.r); err != nil {
			return
		}

		buf := make([]byte, l)
		_, err = d.r.Read(buf)
		return i.UnmarshalBinary(buf)
	}

	// Otherwise, use reflection.
	rv := reflect.Indirect(reflect.ValueOf(v))
	if !rv.CanAddr() {
		return errors.New("binary: can only decode to pointer type")
	}
	t := rv.Type()

	switch t.Kind() {
	case reflect.Array:
		len := t.Len()
		for i := 0; i < int(len); i++ {
			if err = d.decode(rv.Index(i).Addr().Interface()); err != nil {
				return
			}
		}

	case reflect.Slice:
		var l uint64
		if l, err = binary.ReadUvarint(d.r); err != nil {
			return
		}
		if t.Kind() == reflect.Slice {
			rv.Set(reflect.MakeSlice(t, int(l), int(l)))
		} else if int(l) != t.Len() {
			return fmt.Errorf("binary: encoded size %d != real size %d", l, t.Len())
		}
		for i := 0; i < int(l); i++ {
			if err = d.decode(rv.Index(i).Addr().Interface()); err != nil {
				return
			}
		}

	case reflect.Struct:
		l := rv.NumField()
		for i := 0; i < l; i++ {
			if v := rv.Field(i); v.CanSet() && t.Field(i).Name != "_" {
				if !d.decodeValue && t.Field(i).Name == "Value" {
					d.decodeValue = true
				}

				if err = d.decode(v.Addr().Interface()); err != nil {
					return
				}

				if d.decodeValue {
					d.decodeValue = false
				}
			}
		}

	case reflect.Map:
		var l uint64
		if l, err = binary.ReadUvarint(d.r); err != nil {
			return
		}
		kt := t.Key()
		vt := t.Elem()
		rv.Set(reflect.MakeMap(t))
		for i := 0; i < int(l); i++ {
			kv := reflect.Indirect(reflect.New(kt))
			if err = d.decode(kv.Addr().Interface()); err != nil {
				return
			}
			vv := reflect.Indirect(reflect.New(vt))
			if err = d.decode(vv.Addr().Interface()); err != nil {
				return
			}
			rv.SetMapIndex(kv, vv)
		}

	case reflect.Interface:
		if d.decodeValue {
			vv := reflect.Indirect(reflect.New(reflect.TypeOf(d.value)))
			if err = d.decode(vv.Addr().Interface()); err != nil {
				return
			}
			rv.Set(vv)
		}

	case reflect.String:
		var l uint64
		if l, err = binary.ReadUvarint(d.r); err != nil {
			return
		}
		buf := make([]byte, l)
		_, err = d.r.Read(buf)
		rv.SetString(string(buf))

	case reflect.Bool:
		var out byte
		err = binary.Read(d.r, d.Order, &out)
		rv.SetBool(out != 0)

	case reflect.Int:
		var out int64
		err = binary.Read(d.r, d.Order, &out)
		rv.SetInt(out)

	case reflect.Uint:
		var out uint64
		err = binary.Read(d.r, d.Order, &out)
		rv.SetUint(out)

	case reflect.Int8, reflect.Uint8, reflect.Int16, reflect.Uint16,
		reflect.Int32, reflect.Uint32, reflect.Int64, reflect.Uint64,
		reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128:
		err = binary.Read(d.r, d.Order, v)

	default:
		return errors.New("binary: unsupported type " + t.String())
	}
	return
}
