// Package signaling defines the transport-independent signaling boundary.
// Implementations map a room and message kind to their native routing key
// (for example an MQTT topic, NATS subject, Centrifugo channel, or WebSocket
// path).
package signaling

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// MessageType identifies a signaling message without coupling the peer layer
// to any broker-specific routing convention.
type MessageType string

const (
	MessageHello  MessageType = "hello"
	MessageOffer  MessageType = "offer"
	MessageAnswer MessageType = "answer"
)

// Handler receives one opaque, encrypted signaling packet.
type Handler func([]byte)

// Subscription is an independently cancellable signaling subscription.
type Subscription interface {
	Unsubscribe() error
}

// Adapter is the sole abstraction used by the peer layer. A new signaling
// protocol only needs to implement these five methods.
type Adapter interface {
	Connect() error
	Disconnect() error
	Publish(roomID string, typ MessageType, payload []byte) error
	Subscribe(roomID string, typ MessageType, handler Handler) (Subscription, error)
}

// Identifiable is implemented by built-in adapters so a process can enforce
// the one-server-per-room invariant even when separate adapter instances point
// at the same signaling endpoint.
type Identifiable interface {
	SignalingID() string
}

// IAdapter is kept as an alias for source compatibility with callers that
// used the old name. Its method set is the new generic adapter contract.
type IAdapter = Adapter

var ErrClosed = errors.New("signaling adapter is closed")

var roomPattern = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)
var messageTypePattern = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

const (
	MaxRoomIDLength      = 128
	MaxMessageTypeLength = 32
	MaxRoutePrefixLength = 128
)

// ValidateRoomID keeps a room safe for all supported native routing schemes.
// In particular, it prevents MQTT/NATS wildcard injection and guarantees that
// the same value can be used as one WebSocket path segment.
func ValidateRoomID(roomID string) error {
	if len(roomID) == 0 || len(roomID) > MaxRoomIDLength {
		return fmt.Errorf("invalid room ID length: must be between 1 and %d bytes", MaxRoomIDLength)
	}
	if !roomPattern.MatchString(roomID) {
		return fmt.Errorf("invalid room ID %q: use letters, digits, underscore, or hyphen", roomID)
	}
	return nil
}

// ValidateRoutePrefix validates the adapter-specific path preceding roomID
// and message type. Each segment uses the same conservative character set as
// room IDs, preventing MQTT/NATS wildcard and hierarchy injection.
func ValidateRoutePrefix(prefix string, separator rune) error {
	if len(prefix) == 0 || len(prefix) > MaxRoutePrefixLength {
		return fmt.Errorf("invalid route prefix length: must be between 1 and %d bytes", MaxRoutePrefixLength)
	}
	if separator != '/' && separator != '.' && separator != ':' {
		return fmt.Errorf("unsupported route prefix separator %q", separator)
	}
	for _, segment := range strings.Split(prefix, string(separator)) {
		if !roomPattern.MatchString(segment) {
			return fmt.Errorf("invalid route prefix %q: each segment must use letters, digits, underscore, or hyphen", prefix)
		}
	}
	return nil
}

func ValidateMessageType(typ MessageType) error {
	if len(typ) == 0 || len(typ) > MaxMessageTypeLength {
		return fmt.Errorf("invalid signaling message type length: must be between 1 and %d bytes", MaxMessageTypeLength)
	}
	if !messageTypePattern.MatchString(string(typ)) {
		return fmt.Errorf("invalid signaling message type %q", typ)
	}
	return nil
}

type subscription struct {
	unsubscribe func() error
}

func (s *subscription) Unsubscribe() error {
	if s == nil || s.unsubscribe == nil {
		return nil
	}
	return s.unsubscribe()
}

// NewSubscription helps adapter implementations expose their native
// unsubscribe operation without leaking protocol-specific handles.
func NewSubscription(unsubscribe func() error) Subscription {
	return &subscription{unsubscribe: unsubscribe}
}
