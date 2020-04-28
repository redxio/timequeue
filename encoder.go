package timequeue

import (
	"bytes"
)

type encoder struct {
	binaryEncoder *binaryEncoder
	buf           *bytes.Buffer
	in            chan *node
	out           chan []byte
	done          chan semaphore
}

func newEncoder() *encoder {
	buf := new(bytes.Buffer)

	return &encoder{
		binaryEncoder: newBinaryEncoder(buf),
		buf:           buf,
		in:            make(chan *node),
		out:           make(chan []byte),
		done:          make(chan semaphore),
	}
}

func (enc *encoder) service() {
	for n := range enc.in {
		if err := enc.binaryEncoder.encode(n.item); err != nil {
			panic(err)
		}

		enc.out <- enc.buf.Bytes()
		<-enc.done
		enc.buf.Reset()
	}
}
