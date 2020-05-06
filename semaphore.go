package timequeue

type semaphore struct {
	buffered         bool
	signal           internalSignal
	ch               chan internalSignal
	processedSignals map[internalSignal]bool
	actions          map[internalSignal]func()
	broadcasts       map[internalSignal][]*semaphore
}

func newSemaphore(buf int) *semaphore {
	buffered := buf > 0

	return &semaphore{
		buffered:         buffered,
		ch:               make(chan internalSignal, buf),
		processedSignals: make(map[internalSignal]bool),
		actions:          make(map[internalSignal]func()),
		broadcasts:       make(map[internalSignal][]*semaphore),
	}
}

func (sema *semaphore) channel() <-chan internalSignal { return sema.ch }

func (sema *semaphore) addr() *internalSignal { return &sema.signal }

func (sema *semaphore) peek(expected internalSignal) {
	for sema.signal = range sema.ch {
		sema.doAction()

		if sema.signal == expected {
			break
		}
	}
}

func (sema *semaphore) view() internalSignal { return sema.signal }

func (sema *semaphore) doAction() {
	if sema.processedSignals[sema.signal] {
		action, ok := sema.actions[sema.signal]
		if ok {
			action()
		}
	}
}

func (sema *semaphore) processAvailableSignal() {
	if sema.buffered && len(sema.ch) > 0 {
		sema.signal = <-sema.ch
		sema.doAction()
	}
}

func (sema *semaphore) send(signal internalSignal) *semaphore {
	sema.ch <- signal
	return sema
}

func (sema *semaphore) processedSignal(signals ...internalSignal) *semaphore {
	for k := range sema.processedSignals {
		delete(sema.processedSignals, k)
	}

	for _, signal := range signals {
		sema.processedSignals[signal] = true
	}

	return sema
}

func (sema *semaphore) register(signal internalSignal, action func()) *semaphore {
	sema.actions[signal] = action
	return sema
}

func (sema *semaphore) deRegister(signal internalSignal) *semaphore {
	delete(sema.actions, signal)
	return sema
}

func (sema *semaphore) expect(signal internalSignal) bool { return sema.signal == signal }

func (sema *semaphore) broadcastTo(target *semaphore, signal internalSignal) *semaphore {
	targets, ok := sema.broadcasts[signal]
	if ok {
		for _, t := range targets {
			if target == t {
				return sema
			}
		}
	}
	sema.broadcasts[signal] = append(sema.broadcasts[signal], target)

	return sema
}

func (sema *semaphore) broadcast(signal internalSignal) *semaphore {
	targets, ok := sema.broadcasts[signal]
	if ok {
		for _, target := range targets {
			target.send(signal)
		}
	}

	return sema
}
