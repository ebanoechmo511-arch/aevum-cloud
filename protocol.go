package main

import (
	"encoding/binary"
	"errors"
)

// Binary packet IDs (Zenith-compatible voice stream)
const (
	PacketVoiceUp   = 0x23 // 35: Client -> Server
	PacketVoiceDown = 0x10 // 16: Server -> Client
	PacketVoicePing = 0x24 // 36: Voice status / ping
)

// Voice flags
const (
	FlagRadio    = 1 << 0 // 1: Рация (слышно всем без расстояния)
	FlagEnd      = 1 << 1 // 2: Конец фразы (заглушка)
	FlagParty    = 1 << 2 // 4: Канал пати
	FlagRoom     = 1 << 3 // 8: Канал комнаты
	FlagWhisper  = 1 << 4 // 16: Шепот
)

// VoiceFrameUp incoming voice frame from speaking client
type VoiceFrameUp struct {
	Flags    byte
	Range    byte
	Seq      uint16
	OpusData []byte
}

func ParseVoiceUp(data []byte) (*VoiceFrameUp, error) {
	if len(data) < 5 {
		return nil, errors.New("voice frame too short")
	}
	if data[0] != PacketVoiceUp {
		return nil, errors.New("invalid voice packet id")
	}
	flags := data[1]
	voiceRange := data[2]
	seq := binary.BigEndian.Uint16(data[3:5])
	opusData := data[5:]

	return &VoiceFrameUp{
		Flags:    flags,
		Range:    voiceRange,
		Seq:      seq,
		OpusData: opusData,
	}, nil
}

// BuildVoiceDown packs a voice frame to be routed to listeners
func BuildVoiceDown(senderUID uint64, flags byte, voiceRange byte, seq uint16, opusData []byte) []byte {
	out := make([]byte, 1+8+1+1+2+len(opusData))
	out[0] = PacketVoiceDown
	binary.BigEndian.PutUint64(out[1:9], senderUID)
	out[9] = flags
	out[10] = voiceRange
	binary.BigEndian.PutUint16(out[11:13], seq)
	copy(out[13:], opusData)
	return out
}

// JSON Message formats
type TextMsgIn struct {
	Type     string `json:"type"` // "auth", "chat", "state", "party", "waypoint"
	Username string `json:"username,omitempty"`
	HWID     string `json:"hwid,omitempty"`
	ServerIP string `json:"server_ip,omitempty"`

	// Chat
	Channel  string `json:"channel,omitempty"` // "global", "server", "party", "dm"
	Target   string `json:"target,omitempty"`
	Message  string `json:"message,omitempty"`

	// State / Radar sync
	X          float64 `json:"x,omitempty"`
	Y          float64 `json:"y,omitempty"`
	Z          float64 `json:"z,omitempty"`
	Yaw        float32 `json:"yaw,omitempty"`
	Pitch      float32 `json:"pitch,omitempty"`
	Health     float32 `json:"health,omitempty"`
	MaxHealth  float32 `json:"max_health,omitempty"`
	Absorption float32 `json:"absorption,omitempty"`
	Totems     int     `json:"totems,omitempty"`
	Gapples    int     `json:"gapples,omitempty"`
	HelmetDur  float32 `json:"helmet_dur,omitempty"`
	ChestDur   float32 `json:"chest_dur,omitempty"`
	LegsDur    float32 `json:"legs_dur,omitempty"`
	BootsDur   float32 `json:"boots_dur,omitempty"`

	// Waypoints
	Action   string `json:"action,omitempty"`
	WpID     string `json:"wp_id,omitempty"`
	WpName   string `json:"wp_name,omitempty"`
	WpColor  int    `json:"wp_color,omitempty"`
}

type TextMsgOut struct {
	Type      string      `json:"type"`
	Timestamp int64       `json:"timestamp,omitempty"`
	SenderUID uint64      `json:"sender_uid,omitempty"`
	Sender    string      `json:"sender,omitempty"`
	Channel   string      `json:"channel,omitempty"`
	Target    string      `json:"target,omitempty"`
	Message   string      `json:"message,omitempty"`
	Data      interface{} `json:"data,omitempty"`
	Online    int         `json:"online,omitempty"`
}
