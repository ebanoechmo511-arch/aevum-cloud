package main

import (
	"encoding/json"
	"log"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

type Hub struct {
	clients    map[*Client]bool
	clientsMu  sync.RWMutex
	register   chan *Client
	unregister chan *Client

	nextUID uint64
}

func NewHub() *Hub {
	return &Hub{
		clients:    make(map[*Client]bool),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		nextUID:    1000,
	}
}

func (h *Hub) Run() {
	radarTicker := time.NewTicker(250 * time.Millisecond) // 4 Hz state sync for teammates
	defer radarTicker.Stop()

	for {
		select {
		case c := <-h.register:
			h.clientsMu.Lock()
			c.uid = atomic.AddUint64(&h.nextUID, 1)
			h.clients[c] = true
			h.clientsMu.Unlock()
			log.Printf("[+] Подключен сокет (UID: %d)", c.uid)

		case c := <-h.unregister:
			h.clientsMu.Lock()
			if _, ok := h.clients[c]; ok {
				delete(h.clients, c)
				close(c.sendText)
				close(c.sendBin)
				log.Printf("[-] Отключен юзер %s (UID: %d)", c.username, c.uid)
			}
			h.clientsMu.Unlock()
			h.broadcastOnline()

		case <-radarTicker.C:
			h.broadcastRadar()
		}
	}
}

func (h *Hub) broadcastOnline() {
	h.clientsMu.RLock()
	online := len(h.clients)
	h.clientsMu.RUnlock()

	msg, _ := json.Marshal(TextMsgOut{
		Type:   "online_count",
		Online: online,
	})
	h.broadcastAllText(msg)
}

func (h *Hub) broadcastAllText(data []byte) {
	h.clientsMu.RLock()
	defer h.clientsMu.RUnlock()
	for c := range h.clients {
		if c.authorized {
			select {
			case c.sendText <- data:
			default:
			}
		}
	}
}

func (h *Hub) handleText(c *Client, payload []byte) {
	var in TextMsgIn
	if err := json.Unmarshal(payload, &in); err != nil {
		return
	}

	switch in.Type {
	case "auth":
		c.mu.Lock()
		c.username = in.Username
		c.hwid = in.HWID
		c.serverIP = in.ServerIP
		c.authorized = true
		c.mu.Unlock()

		// Welcome response with assigned UID
		resp, _ := json.Marshal(TextMsgOut{
			Type:      "auth_success",
			SenderUID: c.uid,
			Sender:    c.username,
			Timestamp: time.Now().UnixMilli(),
		})
		c.sendText <- resp

		log.Printf("[Auth] Юзер: %s (HWID: %s, Server: %s)", in.Username, in.HWID, in.ServerIP)
		h.broadcastOnline()

		// Alert users on the same Minecraft server
		if in.ServerIP != "" {
			h.alertServerUsers(c, in.ServerIP)
		}

	case "server_change":
		c.mu.Lock()
		c.serverIP = in.ServerIP
		c.mu.Unlock()
		if in.ServerIP != "" {
			h.alertServerUsers(c, in.ServerIP)
		}

	case "chat":
		if !c.authorized || in.Message == "" {
			return
		}
		h.routeChat(c, in)

	case "state":
		if !c.authorized {
			return
		}
		c.mu.Lock()
		c.x = in.X
		c.y = in.Y
		c.z = in.Z
		c.health = in.Health
		c.maxHealth = in.MaxHealth
		c.absorption = in.Absorption
		c.totems = in.Totems
		c.gapples = in.Gapples
		c.helmetDur = in.HelmetDur
		c.chestDur = in.ChestDur
		c.legsDur = in.LegsDur
		c.bootsDur = in.BootsDur
		c.mu.Unlock()

	case "party":
		if !c.authorized {
			return
		}
		h.handleParty(c, in)

	case "waypoint":
		if !c.authorized {
			return
		}
		h.routeWaypoint(c, in)
	}
}

func (h *Hub) handleBinary(c *Client, payload []byte) {
	if !c.authorized {
		return
	}

	frame, err := ParseVoiceUp(payload)
	if err != nil {
		return
	}

	downPacket := BuildVoiceDown(c.uid, frame.Flags, frame.Range, frame.Seq, frame.OpusData)

	c.mu.RLock()
	senderServer := c.serverIP
	senderParty := c.partyID
	c.mu.RUnlock()

	h.clientsMu.RLock()
	defer h.clientsMu.RUnlock()

	for target := range h.clients {
		if target == c || !target.authorized {
			continue
		}

		target.mu.RLock()
		targetServer := target.serverIP
		targetParty := target.partyID
		target.mu.RUnlock()

		// Party channel routing
		if (frame.Flags & FlagParty) != 0 {
			if senderParty != "" && senderParty == targetParty {
				h.sendBinSafe(target, downPacket)
			}
			continue
		}

		// Radio routing
		if (frame.Flags & FlagRadio) != 0 {
			if senderServer != "" && senderServer == targetServer {
				h.sendBinSafe(target, downPacket)
			}
			continue
		}

		// Standard Proximity: deliver to all users on same Minecraft server
		// Client does accurate 3D distance falloff, occlusion raycast and HRTF
		if senderServer != "" && senderServer == targetServer {
			h.sendBinSafe(target, downPacket)
		}
	}
}

func (h *Hub) sendBinSafe(c *Client, data []byte) {
	select {
	case c.sendBin <- data:
	default:
	}
}

func (h *Hub) routeChat(sender *Client, in TextMsgIn) {
	out := TextMsgOut{
		Type:      "chat",
		Timestamp: time.Now().UnixMilli(),
		SenderUID: sender.uid,
		Sender:    sender.username,
		Channel:   in.Channel,
		Target:    in.Target,
		Message:   in.Message,
	}
	bytes, _ := json.Marshal(out)

	sender.mu.RLock()
	senderServer := sender.serverIP
	senderParty := sender.partyID
	sender.mu.RUnlock()

	h.clientsMu.RLock()
	defer h.clientsMu.RUnlock()

	switch in.Channel {
	case "global":
		for c := range h.clients {
			if c.authorized {
				c.sendText <- bytes
			}
		}

	case "server":
		for c := range h.clients {
			if c.authorized && c.serverIP == senderServer {
				c.sendText <- bytes
			}
		}

	case "party":
		if senderParty == "" {
			return
		}
		for c := range h.clients {
			if c.authorized && c.partyID == senderParty {
				c.sendText <- bytes
			}
		}

	case "dm":
		for c := range h.clients {
			if c.authorized && (c.username == in.Target || c == sender) {
				c.sendText <- bytes
			}
		}
	}
}

func (h *Hub) alertServerUsers(newUser *Client, serverIP string) {
	h.clientsMu.RLock()
	defer h.clientsMu.RUnlock()

	for c := range h.clients {
		if c != newUser && c.authorized && c.serverIP == serverIP {
			alert, _ := json.Marshal(TextMsgOut{
				Type:      "user_alert",
				Message:   "Юзер AevumDLC " + newUser.username + " играет на твоем сервере!",
				Sender:    newUser.username,
				Timestamp: time.Now().UnixMilli(),
			})
			select {
			case c.sendText <- alert:
			default:
			}
		}
	}
}

func (h *Hub) handleParty(c *Client, in TextMsgIn) {
	c.mu.Lock()
	switch in.Action {
	case "create":
		c.partyID = c.username
	case "join":
		c.partyID = in.Target
	case "leave":
		c.partyID = ""
	}
	partyID := c.partyID
	c.mu.Unlock()

	resp, _ := json.Marshal(TextMsgOut{
		Type:    "party_update",
		Channel: partyID,
	})
	c.sendText <- resp
}

func (h *Hub) routeWaypoint(sender *Client, in TextMsgIn) {
	sender.mu.RLock()
	partyID := sender.partyID
	sender.mu.RUnlock()

	if partyID == "" {
		return
	}

	bytes, _ := json.Marshal(TextMsgOut{
		Type:      "waypoint",
		Sender:    sender.username,
		Data:      in,
		Timestamp: time.Now().UnixMilli(),
	})

	h.clientsMu.RLock()
	defer h.clientsMu.RUnlock()

	for c := range h.clients {
		if c.authorized && c.partyID == partyID {
			select {
			case c.sendText <- bytes:
			default:
			}
		}
	}
}

type TeammateState struct {
	UID        uint64  `json:"uid"`
	Username   string  `json:"username"`
	X          float64 `json:"x"`
	Y          float64 `json:"y"`
	Z          float64 `json:"z"`
	Health     float32 `json:"health"`
	MaxHealth  float32 `json:"max_health"`
	Absorption float32 `json:"absorption"`
	Totems     int     `json:"totems"`
	Gapples    int     `json:"gapples"`
	HelmetDur  float32 `json:"helmet_dur"`
	ChestDur   float32 `json:"chest_dur"`
	LegsDur    float32 `json:"legs_dur"`
	BootsDur   float32 `json:"boots_dur"`
}

func (h *Hub) broadcastRadar() {
	h.clientsMu.RLock()
	defer h.clientsMu.RUnlock()

	// Group clients by party
	parties := make(map[string][]*Client)
	for c := range h.clients {
		if c.authorized && c.partyID != "" {
			parties[c.partyID] = append(parties[c.partyID], c)
		}
	}

	for _, members := range parties {
		states := make([]TeammateState, 0, len(members))
		for _, m := range members {
			m.mu.RLock()
			states = append(states, TeammateState{
				UID:        m.uid,
				Username:   m.username,
				X:          m.x,
				Y:          m.y,
				Z:          m.z,
				Health:     m.health,
				MaxHealth:  m.maxHealth,
				Absorption: m.absorption,
				Totems:     m.totems,
				Gapples:    m.gapples,
				HelmetDur:  m.helmetDur,
				ChestDur:   m.chestDur,
				LegsDur:    m.legsDur,
				BootsDur:   m.bootsDur,
			})
			m.mu.RUnlock()
		}

		payload, _ := json.Marshal(TextMsgOut{
			Type: "radar_sync",
			Data: states,
		})

		for _, m := range members {
			select {
			case m.sendText <- payload:
			default:
			}
		}
	}
}

func dist(x1, y1, z1, x2, y2, z2 float64) float64 {
	dx := x1 - x2
	dy := y1 - y2
	dz := z1 - z2
	return math.Sqrt(dx*dx + dy*dy + dz*dz)
}
