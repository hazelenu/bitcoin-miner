package lsp

type SlidingWindow struct {
	params    *Params
	entries   []*entry
	len       int
	unAcked   int
	nextAckSn int
}

type SlidingWindowParams struct {
	params *Params
	sn     int
}

type SlidingWindowErrorType int

const (
	SWEConnectionLost SlidingWindowErrorType = iota
)

type SlidingWindowError struct {
	errorType SlidingWindowErrorType
	message   string
}

func (e *SlidingWindowError) Error() string {
	return e.message
}

type entry struct {
	ack          bool     // Has this entry ACKed by remote?
	start        int      // Epoch timestamp in which this entry is sent
	nextRetry    int      // Epoch timestamp in which this entry should be resent if not yet ACKed
	nextRetryGap int      // How much time gap should the next retry be?
	message      *Message // Original message payload
}

func NewSlidingWindow(swParams *SlidingWindowParams) *SlidingWindow {
	sw := &SlidingWindow{
		params:    swParams.params,
		entries:   make([]*entry, swParams.params.WindowSize),
		len:       0,
		unAcked:   0,
		nextAckSn: swParams.sn,
	}
	return sw
}

func (sw *SlidingWindow) CanPlace(sn int) bool {
	return sw.len < sw.params.WindowSize && sn < sw.nextAckSn+sw.params.WindowSize && sw.unAcked < sw.params.MaxUnackedMessages
}

func (sw *SlidingWindow) Place(message *Message, epoch int) {
	idx := message.SeqNum % sw.params.WindowSize
	if sw.entries[idx] != nil {
		panic("Attempt to place message but slot still taken; use CanPlace() to check before calling Place()")
	}
	// Make the time limit a bit more lenient
	sw.len++
	sw.entries[idx] = &entry{
		ack:          false,
		start:        epoch,
		nextRetry:    epoch + 1,
		nextRetryGap: min(1, sw.params.MaxBackOffInterval),
		message:      message,
	}
	sw.unAcked++
}

func (sw *SlidingWindow) Ack(sn int) {
	if sn < sw.nextAckSn {
		// Already out of window
		return
	}
	if sn >= sw.nextAckSn+sw.params.WindowSize {
		panic("Received ACK beyond current window")
	}
	e := sw.entries[sn%sw.params.WindowSize]
	if e == nil {
		panic("Received ACK within range but slot is empty")
	}
	if e.ack {
		// Already acked
		return
	}

	e.ack = true
	sw.unAcked--
	if sn == sw.nextAckSn {
		sw.cleanup()
	}
}

/*
*
cleanup

Clean up consecutive acked slots from the beginning of the window.
*/
func (sw *SlidingWindow) cleanup() {
	for sw.len > 0 {
		idx := sw.nextAckSn % sw.params.WindowSize
		if sw.entries[idx] == nil {
			panic("Attempt to clean up but a slot within range is empty")
		}
		if !sw.entries[idx].ack {
			break
		}
		sw.entries[idx] = nil
		sw.nextAckSn++
		sw.len--
	}
}

func (sw *SlidingWindow) CAck(sn int) {
	if sn < sw.nextAckSn {
		// Already out of window
		return
	}
	if sn >= sw.nextAckSn+sw.params.WindowSize {
		panic("Received ACK beyond current window")
	}
	for i := sw.nextAckSn; i <= sn; i++ {
		e := sw.entries[i%sw.params.WindowSize]
		if e == nil {
			panic("Received ACK within range but slot is empty")
		}
		if e.ack {
			// Already acked
			continue
		}

		e.ack = true
		sw.unAcked--
	}
	sw.cleanup()
}

/*
GetMessagesToResend

Returns a slice of messages that needs to be resent
*/
func (sw *SlidingWindow) GetMessagesToResend(epoch int) ([]*Message, *SlidingWindowError) {
	ret := make([]*Message, 0)
	for i := sw.nextAckSn; i < sw.nextAckSn+sw.len; i++ {
		e := sw.entries[i%sw.params.WindowSize]
		if e == nil {
			panic("Attempt to check epoch but a slot within range is empty")
		}
		if e.ack {
			continue
		}
		if epoch-e.start >= sw.params.EpochLimit {
			return nil, &SlidingWindowError{
				errorType: SWEConnectionLost,
				message:   "One or more entries have timed out",
			}
		}
		if epoch >= e.nextRetry {
			ret = append(ret, e.message)
			e.nextRetry = epoch + e.nextRetryGap + 1
			e.nextRetryGap = min(e.nextRetryGap*2, sw.params.MaxBackOffInterval)
		}
	}
	return ret, nil
}
