package timequeue

type internalSignal int

const (
	defaultInternalSignal internalSignal = iota
	consumption
	reconsumption
	done
	stop
	resume
)
