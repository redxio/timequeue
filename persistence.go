package timequeue

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	expiredMetaInfoLength = int64(binary.MaxVarintLen64)
	validMetaInfoLength   = int64(binary.MaxVarintLen64)
	fileMetaInfoLength    = expiredMetaInfoLength + validMetaInfoLength

	validMetaInfoOffset = expiredMetaInfoLength
	expiredDataOffset   = fileMetaInfoLength
)

type node struct {
	length int64
	item   item
}

type blockinfo struct {
	data   []byte
	offset int64
}

type persistence struct {
	file          *os.File
	encoder       *encoder
	expiredLength int64
	validLength   int64
	stream        chan *blockinfo
	expired       chan int64
	maxExpired    int64 // maximum amount of expired bytes
	clean         chan int64
	fileSize      int64
	mMapLength    int64
	mMap          []byte
	osSignal      chan os.Signal
	value         interface{}
	registry      map[string]interface{}
	worker        *semaphore
}

func newPersistence() *persistence {
	return &persistence{
		stream:   make(chan *blockinfo),
		expired:  make(chan int64),
		clean:    make(chan int64, 1),
		osSignal: make(chan os.Signal, 1),
	}
}

func (p *persistence) getFileSize() (int64, error) {
	fstat, err := p.file.Stat()
	if err != nil {
		return 0, err
	}

	return fstat.Size(), nil
}

func (p *persistence) Mmap(writtenLength int64) error {
	fileSize, err := p.getFileSize()
	if err != nil {
		return err
	}
	p.fileSize = fileSize

	if p.fileSize > 0 {
		p.mMapLength = p.fileSize + writtenLength
	} else {
		p.mMapLength = fileMetaInfoLength + writtenLength
	}

	if p.mMapLength != p.fileSize {
		if err = syscall.Ftruncate(int(p.file.Fd()), p.mMapLength); err != nil {
			return err
		}
	}

	mMap, err := syscall.Mmap(int(p.file.Fd()), 0, int(p.mMapLength), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return err
	}

	p.mMap = mMap
	return nil
}

func (p *persistence) Munmap() error {
	if p.fileSize != p.mMapLength {
		if err := syscall.Ftruncate(int(p.file.Fd()), p.fileSize); err != nil {
			return err
		}
	}

	if err := syscall.Munmap(p.mMap); err != nil {
		return err
	}

	p.mMap = nil
	return nil
}

func (p *persistence) modifyMetaInfo(newExpiredLength int64) {
	p.expiredLength += newExpiredLength
	validLength := atomic.AddInt64(&p.validLength, -newExpiredLength)

	binary.LittleEndian.PutUint64(p.mMap[:expiredMetaInfoLength], uint64(p.expiredLength))
	binary.LittleEndian.PutUint64(p.mMap[validMetaInfoOffset:fileMetaInfoLength], uint64(validLength))
}

func (p *persistence) writeData(block *blockinfo, cleanExpired bool) {
	writtenLength := int64(len(block.data))

	if cleanExpired {
		writtenStart := expiredDataOffset
		readStart := writtenStart + p.expiredLength
		copy(p.mMap[writtenStart:], p.mMap[readStart:readStart+block.offset])

		writtenStart += block.offset
		readStart += block.offset

		if writtenStart+writtenLength > readStart {
			backupValidData := make([]byte, p.fileSize-readStart)
			copy(backupValidData, p.mMap[readStart:])

			copy(p.mMap[writtenStart:], block.data[:])
			writtenStart += writtenLength
			copy(p.mMap[writtenStart:], backupValidData[:])
		} else {
			copy(p.mMap[writtenStart:], block.data[:])
			writtenStart += writtenLength
			copy(p.mMap[writtenStart:], p.mMap[readStart:])
		}

		p.fileSize = p.mMapLength - p.expiredLength

		p.expiredLength = 0
		validLength := atomic.AddInt64(&p.validLength, writtenLength)

		binary.LittleEndian.PutUint64(p.mMap[:expiredMetaInfoLength], uint64(p.expiredLength))
		binary.LittleEndian.PutUint64(p.mMap[validMetaInfoLength:fileMetaInfoLength], uint64(validLength))
	} else {
		readStart := expiredDataOffset + p.expiredLength + block.offset
		writtenStart := readStart

		validLength := atomic.LoadInt64(&p.validLength)
		if validLength > block.offset {
			backupValidData := make([]byte, validLength-block.offset)
			copy(backupValidData[:], p.mMap[readStart:])

			copy(p.mMap[writtenStart:], block.data[:])
			writtenStart += writtenLength
			copy(p.mMap[writtenStart:], backupValidData[:])
		} else {
			copy(p.mMap[writtenStart:], block.data[:])
		}

		validLength = atomic.AddInt64(&p.validLength, writtenLength)
		binary.LittleEndian.PutUint64(p.mMap[validMetaInfoLength:fileMetaInfoLength], uint64(validLength))

		p.fileSize = p.mMapLength
	}
}

func (p *persistence) cleanExpired(newExpiredLength int64) {
	if newExpiredLength != 0 {
		p.expiredLength += newExpiredLength
		validLength := atomic.AddInt64(&p.validLength, -newExpiredLength)
		binary.LittleEndian.PutUint64(p.mMap[validMetaInfoOffset:fileMetaInfoLength], uint64(validLength))
	}

	writtenStart := expiredDataOffset
	readStart := writtenStart + p.expiredLength

	copy(p.mMap[writtenStart:], p.mMap[readStart:])

	p.fileSize = p.mMapLength - p.expiredLength
	p.expiredLength = 0
	binary.LittleEndian.PutUint64(p.mMap[:expiredMetaInfoLength], 0)
}

func (tq *TimeQueue) withPersistence(filename string, maxExpired int64, value interface{}, registry map[string]interface{}) error {
	if maxExpired < 0 {
		return errors.New("maximum expired length is less than 0")
	}

	file, err := createOrOpenFile(filename)
	if err != nil {
		return err
	}

	persistence := newPersistence()
	encoder := newEncoder()

	pWorker := newSemaphore(0)
	eWorker := newSemaphore(0)

	pWorker.broadcastTo(eWorker, done)
	pWorker.broadcastTo(eWorker, stop)

	persistence.worker = pWorker
	encoder.worker = eWorker

	persistence.file = file
	persistence.value = value
	persistence.encoder = encoder
	persistence.registry = registry
	persistence.maxExpired = maxExpired

	tq.persistence = persistence
	tq.persistent = true

	signal.Notify(persistence.osSignal, syscall.SIGINT, syscall.SIGTERM)

	fileSize, err := persistence.getFileSize()
	if err != nil {
		return err
	}

	if fileSize > 0 {
		if fileSize < fileMetaInfoLength {
			return fmt.Errorf("corrupted file %s: file size is less than %d bytes", filename, fileMetaInfoLength)
		}

		if fileSize >= fileMetaInfoLength {
			mMap, err := syscall.Mmap(int(persistence.file.Fd()), 0, int(fileSize), syscall.PROT_READ, syscall.MAP_SHARED)
			if err != nil {
				return err
			}

			expiredLength := int64(binary.LittleEndian.Uint64(mMap[:expiredMetaInfoLength]))
			validLength := int64(binary.LittleEndian.Uint64(mMap[validMetaInfoOffset:fileMetaInfoLength]))

			if fileMetaInfoLength+expiredLength+validLength != fileSize {
				return fmt.Errorf("corrupted file %s: failed to validate integrity", filename)
			}

			if expiredLength > 0 {
				persistence.expiredLength = expiredLength
			}

			var data []byte

			if validLength > 0 {
				persistence.validLength = validLength

				data = make([]byte, validLength)
				copy(data[:], mMap[expiredDataOffset+expiredLength:])
			}

			if err = syscall.Munmap(mMap); err != nil {
				return err
			}

			if len(data) > 0 {
				go tq.loadData(data)
			}
		}
	}

	tq.initWorker()

	go tq.service()
	go tq.persistence.service()
	go tq.persistence.encoder.service()

	return nil
}

func (tq *TimeQueue) initWorker() {
	tq.worker.processedSignal(stop, resume)

	tq.worker.register(stop, func() {
		tq.lock.RLock()

		if tq.persistent {
			if tq.queue.Len() > 0 {
				tq.stopTime = tq.queue.Back().Value.(*node).item.Expire
			} else {
				tq.stopTime = time.Now()
			}
			tq.persistent = false
		}

		tq.lock.RUnlock()
	})
	tq.worker.register(resume, func() {
		tq.lock.RLock()

		if !tq.persistent && !tq.stopTime.IsZero() {
			tq.stopTime = time.Time{}
			tq.persistent = true

			if tq.stopPersistence {
				tq.stopPersistence = false

				go tq.persistence.service()
				go tq.persistence.encoder.service()
			}
		}

		tq.lock.RUnlock()
	})
}

func createOrOpenFile(filename string) (*os.File, error) {
	var f *os.File

	if _, err := os.Stat(filename); os.IsNotExist(err) {
		err = os.MkdirAll(filepath.Dir(filename), 0755)
		if err != nil {
			return nil, err
		}

		f, err = os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return nil, err
		}
	} else {
		f, err = os.OpenFile(filename, os.O_RDWR, 0644)
		if err != nil {
			return nil, err
		}
	}
	return f, nil
}

func (tq *TimeQueue) loadData(data []byte) {
	var (
		err            error
		previousOffset int64
		currentOffset  int64
	)

	r := bytes.NewReader(data)
	dec := newBinaryDecoder(r, tq.persistence.value, tq.persistence.registry)

	for {
		n := &node{}

		if err = dec.decode(&n.item); err != nil {
			if err == io.EOF {
				return
			}
			panic(err)
		}

		currentOffset, err = r.Seek(0, io.SeekCurrent)
		if err != nil {
			panic(err)
		}

		n.length = currentOffset - previousOffset
		previousOffset = currentOffset

		tq.lock.Lock()
		tq.insert(n)
		tq.lock.Unlock()
	}
}

func (p *persistence) service() {

	for {
		select {
		case block := <-p.stream:
			writtenLength := int64(len(block.data))
			if err := p.Mmap(writtenLength); err != nil {
				panic(err)
			}

			p.syncStream(block)
			p.worker.broadcast(done)

			if err := p.Munmap(); err != nil {
				panic(err)
			}

			if len(p.osSignal) > 0 {
				return
			}
		case newExpiredLength := <-p.expired:
			if err := p.Mmap(0); err != nil {
				panic(err)
			}

			p.syncExpired(newExpiredLength)

			if err := p.Munmap(); err != nil {
				panic(err)
			}

			if len(p.osSignal) > 0 {
				return
			}
		case maxExpired := <-p.clean:
			if p.maxExpired != maxExpired {
				p.maxExpired = maxExpired

				if err := p.Mmap(0); err != nil {
					panic(err)
				}

				p.syncExpired(0)

				if err := p.Munmap(); err != nil {
					panic(err)
				}
			}

			if len(p.osSignal) > 0 {
				return
			}
		case <-p.osSignal:
			return
		case *p.worker.addr() = <-p.worker.channel():
			if p.worker.expect(stop) {
				p.worker.broadcast(stop)
				return
			}
		}
	}
}

func (p *persistence) checkLatestMaxExpired() {
	if len(p.clean) > 0 {
		maxExpired := <-p.clean
		if p.maxExpired != maxExpired {
			p.maxExpired = maxExpired
		}
	}
}

func (p *persistence) syncStream(block *blockinfo) {
	if validLength := atomic.LoadInt64(&p.validLength); block.offset > validLength {
		block.offset = validLength
	}

	p.checkLatestMaxExpired()

	// discard all expired data and write new valid data
	if p.expiredLength != 0 && p.expiredLength >= p.maxExpired {
		p.writeData(block, true)
	} else { // write new valid data only
		p.writeData(block, false)
	}
}

func (p *persistence) syncExpired(newExpiredLength int64) {
	totalExpiredLength := p.expiredLength + newExpiredLength

	p.checkLatestMaxExpired()

	// discard all expired data
	if totalExpiredLength != 0 && totalExpiredLength >= p.maxExpired {
		p.cleanExpired(newExpiredLength)
	} else if newExpiredLength != 0 { // only change meta info
		p.modifyMetaInfo(newExpiredLength)
	}
}
