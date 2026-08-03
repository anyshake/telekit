// Package brutal implements Hysteria's fixed-rate congestion controller.
package brutal

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/anyshake/telekit/transports/transport_quic/congestion/common"
	"github.com/apernet/quic-go/congestion"
	"github.com/apernet/quic-go/monotime"
)

const (
	pktInfoSlotCount           = 5
	minSampleCount             = 50
	minAckRate                 = 0.8
	congestionWindowMultiplier = 2

	debugEnv           = "HYSTERIA_BRUTAL_DEBUG"
	debugPrintInterval = 2
)

var _ congestion.CongestionControl = &Sender{}

type Sender struct {
	rttStats        congestion.RTTStatsProvider
	bps             congestion.ByteCount
	maxDatagramSize congestion.ByteCount
	pacer           *common.Pacer

	pktInfoSlots [pktInfoSlotCount]pktInfo
	ackRate      float64

	debug                 bool
	lastAckPrintTimestamp int64
}

type pktInfo struct {
	Timestamp int64
	AckCount  uint64
	LossCount uint64
}

// NewSender creates a fixed-rate sender. bps is measured in bits per second.
func NewSender(bps uint64) *Sender {
	debug, _ := strconv.ParseBool(os.Getenv(debugEnv))
	s := &Sender{
		bps:             congestion.ByteCount(bps),
		maxDatagramSize: congestion.InitialPacketSize,
		ackRate:         1,
		debug:           debug,
	}
	s.pacer = common.NewPacer(func() congestion.ByteCount {
		return congestion.ByteCount(float64(s.bps) / s.ackRate)
	})
	return s
}

func (s *Sender) SetRTTStatsProvider(rttStats congestion.RTTStatsProvider) {
	s.rttStats = rttStats
}

func (s *Sender) TimeUntilSend(bytesInFlight congestion.ByteCount) monotime.Time {
	return s.pacer.TimeUntilSend()
}

func (s *Sender) HasPacingBudget(now monotime.Time) bool {
	return s.pacer.Budget(now) >= s.maxDatagramSize
}

func (s *Sender) CanSend(bytesInFlight congestion.ByteCount) bool {
	return bytesInFlight <= s.GetCongestionWindow()
}

func (s *Sender) GetCongestionWindow() congestion.ByteCount {
	rtt := s.rttStats.SmoothedRTT()
	if rtt <= 0 {
		return 10240
	}
	cwnd := congestion.ByteCount(float64(s.bps) * rtt.Seconds() * congestionWindowMultiplier / s.ackRate)
	if cwnd < s.maxDatagramSize {
		cwnd = s.maxDatagramSize
	}
	return cwnd
}

func (s *Sender) OnPacketSent(sentTime monotime.Time, bytesInFlight congestion.ByteCount,
	packetNumber congestion.PacketNumber, bytes congestion.ByteCount, isRetransmittable bool,
) {
	s.pacer.SentPacket(sentTime, bytes)
}

func (s *Sender) OnPacketAcked(number congestion.PacketNumber, ackedBytes congestion.ByteCount,
	priorInFlight congestion.ByteCount, eventTime monotime.Time,
) {
}

func (s *Sender) OnCongestionEvent(number congestion.PacketNumber, lostBytes congestion.ByteCount,
	priorInFlight congestion.ByteCount,
) {
}

func (s *Sender) OnCongestionEventEx(priorInFlight congestion.ByteCount, eventTime monotime.Time,
	ackedPackets []congestion.AckedPacketInfo, lostPackets []congestion.LostPacketInfo,
) {
	currentTimestamp := int64(time.Duration(eventTime) / time.Second)
	slot := currentTimestamp % pktInfoSlotCount
	if s.pktInfoSlots[slot].Timestamp == currentTimestamp {
		s.pktInfoSlots[slot].LossCount += uint64(len(lostPackets))
		s.pktInfoSlots[slot].AckCount += uint64(len(ackedPackets))
	} else {
		s.pktInfoSlots[slot] = pktInfo{Timestamp: currentTimestamp, AckCount: uint64(len(ackedPackets)), LossCount: uint64(len(lostPackets))}
	}
	s.updateAckRate(currentTimestamp)
}

func (s *Sender) SetMaxDatagramSize(size congestion.ByteCount) {
	s.maxDatagramSize = size
	s.pacer.SetMaxDatagramSize(size)
	if s.debug {
		s.debugPrint("SetMaxDatagramSize: %d", size)
	}
}

func (s *Sender) updateAckRate(currentTimestamp int64) {
	minTimestamp := currentTimestamp - pktInfoSlotCount
	var ackCount, lossCount uint64
	for _, info := range s.pktInfoSlots {
		if info.Timestamp < minTimestamp {
			continue
		}
		ackCount += info.AckCount
		lossCount += info.LossCount
	}
	if ackCount+lossCount < minSampleCount {
		s.ackRate = 1
		return
	}
	rate := float64(ackCount) / float64(ackCount+lossCount)
	if rate < minAckRate {
		s.ackRate = minAckRate
	} else {
		s.ackRate = rate
	}
	if s.debug && currentTimestamp-s.lastAckPrintTimestamp >= debugPrintInterval {
		s.lastAckPrintTimestamp = currentTimestamp
		s.debugPrint("ACK rate: %.2f (total=%d, ack=%d, loss=%d, rtt=%d)", s.ackRate, ackCount+lossCount, ackCount, lossCount, s.rttStats.SmoothedRTT().Milliseconds())
	}
}

func (s *Sender) InSlowStart() bool                                 { return false }
func (s *Sender) InRecovery() bool                                  { return false }
func (s *Sender) MaybeExitSlowStart()                               {}
func (s *Sender) OnRetransmissionTimeout(packetsRetransmitted bool) {}

func (s *Sender) debugPrint(format string, args ...any) {
	fmt.Printf("[BrutalSender] [%s] %s\n", time.Now().Format("15:04:05"), fmt.Sprintf(format, args...))
}
