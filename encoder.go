package timequeue

import (
	"bytes"
)

type encoder struct {
	binaryEncoder *binaryEncoder
	buf           *bytes.Buffer
	in            chan *node
	out           chan []byte
	worker        *semaphore
}

func newEncoder() *encoder {
	buf := new(bytes.Buffer)

	return &encoder{
		binaryEncoder: newBinaryEncoder(buf),
		buf:           buf,
		in:            make(chan *node),
		out:           make(chan []byte),
	}
}

func (enc *encoder) service() {

	for {
		select {
		case n := <-enc.in:
			if err := enc.binaryEncoder.encode(n.item); err != nil {
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
