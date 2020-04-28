package timequeue

type semaphore struct{}

var (
	consumption   semaphore
	reconsumption semaphore
	done          semaphore
)
