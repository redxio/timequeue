package timequeue

import (
	"bytes"
)

type encoder struct {
	binaryEncoder *binaryEncoder
	buf           *bytes.Buffer
	in            chan *item
	out           chan []byte
	worker        *semaphore
}

func newEncoder() *encoder {
	buf := new(bytes.Buffer)

	return &encoder{
		binaryEncoder: newBinaryEncoder(buf),
		buf:           buf,
		in:            make(chan *item),
		out:           make(chan []byte),
	}
}

func (enc *encoder) service() {

	for {
		select {
		case item := <-enc.in:
			if err := enc.binaryEncoder.encode(item); err != nil {
				panic(err)
			}

			enc.out <- enc.buf.Bytes()
			enc.worker.peek(done)
			enc.buf.Reset()

		case *enc.worker.addr() = <-enc.worker.channel():
			if enc.worker.expect(stop) {
				return
			}
		}
	}
}
