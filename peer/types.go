package peer

import "github.com/pion/webrtc/v4"

func ICECandidateSize(candidate *webrtc.ICECandidateInit) int {
	if candidate == nil {
		return 0
	}
	size := len(candidate.Candidate)
	if candidate.SDPMid != nil {
		size += len(*candidate.SDPMid)
	}
	if candidate.UsernameFragment != nil {
		size += len(*candidate.UsernameFragment)
	}
	return size + 16
}

type MessageType string

const (
	MessageTypeClientHello MessageType = "client_hello"
	MessageTypeServerHello MessageType = "server_hello"
	MessageTypeOffer       MessageType = "offer"
	MessageTypeAnswer      MessageType = "answer"
	MessageTypeICE         MessageType = "ice"
)

type Header struct {
	Type     MessageType
	SourceId string
	TargetId string
	Sequence uint64
	Metadata map[string]any
}

type Payload struct {
	SDP                *webrtc.SessionDescription
	ICE                *webrtc.ICECandidateInit
	SessionSalt        []byte
	ClientNonce        []byte
	ServerNonce        []byte
	ClientEphemeralKey []byte
	ServerEphemeralKey []byte
	Signature          []byte
	HandshakeRoomID    string
	HandshakeClientID  string
	Timestamp          []byte
	Data               any
}

type Message struct {
	Header  *Header
	Payload *Payload
	Encrypt bool
}
