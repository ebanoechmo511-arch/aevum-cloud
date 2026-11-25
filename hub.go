package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Hub struct {
	clients    map[*Client]bool
	clientsMu  sync.RWMutex
	register   chan *Client
	unregister chan *Client
	registry   *Registry

	nextUID uint64
}

func NewHub() *Hub {
	return &Hub{
		clients:    make(map[*Client]bool),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		registry:   NewRegistry("registry.json"),
		nextUID:    1,
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
			var offlineNick, offlineIgn string
			var wasAuthed bool
			h.clientsMu.Lock()
			if _, ok := h.clients[c]; ok {
				c.mu.RLock()
				offlineNick = c.clientNick
				offlineIgn = c.gameIGN
				wasAuthed = c.authorized
				c.mu.RUnlock()
				delete(h.clients, c)
				close(c.sendText)
				close(c.sendBin)
				log.Printf("[-] Отключен юзер %s (UID: %d)", c.username, c.uid)
			}
			h.clientsMu.Unlock()
			if wasAuthed && offlineNick != "" {
				offMsg, _ := json.Marshal(TextMsgOut{
					Type:       "user_offline",
					ClientNick: offlineNick,
					GameIGN:    offlineIgn,
					Timestamp:  time.Now().UnixMilli(),
				})
				h.broadcastAllText(offMsg)
			}
			h.broadcastOnline()

		case <-radarTicker.C:
			h.broadcastRadar()
		}
	}
}

func (h *Hub) broadcastOnline() {
	h.clientsMu.RLock()
	seen := make(map[string]bool)
	for c := range h.clients {
		if !c.authorized {
			continue
		}
		c.mu.RLock()
		key := strings.ToLower(strings.TrimSpace(c.clientNick))
		if key == "" {
			key = fmt.Sprintf("uid:%d", c.uid)
		}
		c.mu.RUnlock()
		seen[key] = true
	}
	online := len(seen)
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
	case "claim_nick":
		uid, canonNick, err := h.registry.ClaimNick(in.ClientNick, in.HWID)
		var resp []byte
		if err != nil {
			resp, _ = json.Marshal(TextMsgOut{
				Type:      "claim_result",
				Success:   false,
				Error:     err.Error(),
				Timestamp: time.Now().UnixMilli(),
			})
		} else {
			resp, _ = json.Marshal(TextMsgOut{
				Type:       "claim_result",
				Success:    true,
				ClientNick: canonNick,
				ClientUID:  uid,
				Timestamp:  time.Now().UnixMilli(),
			})
			log.Printf("[NickClaim] Закреплен ник: %s -> UID: %d (HWID: %s)", canonNick, uid, in.HWID)
		}
		c.sendText <- resp

	case "auth":
		c.mu.Lock()
		c.username = in.Username
		if in.ClientNick != "" {
			c.clientNick = in.ClientNick
		} else {
			c.clientNick = in.Username
		}
		if in.ClientUID > 0 {
			c.uid = uint64(in.ClientUID)
		}
		if in.GameIGN != "" {
			c.gameIGN = in.GameIGN
		} else {
			c.gameIGN = in.Username
		}
		c.hwid = in.HWID
		c.serverIP = in.ServerIP
		c.authorized = true
		authNick := c.clientNick
		authHwid := c.hwid
		c.mu.Unlock()

		// Evict any stale duplicate socket from the same HWID or clientNick
		h.clientsMu.Lock()
		for other := range h.clients {
			if other == c {
				continue
			}
			other.mu.RLock()
			sameNick := authNick != "" && strings.EqualFold(other.clientNick, authNick)
			sameHwid := authHwid != "" && other.hwid == authHwid
			other.mu.RUnlock()
			if sameNick || sameHwid {
				delete(h.clients, other)
				close(other.sendText)
				_ = other.conn.Close()
			}
		}
		h.clientsMu.Unlock()

		// Welcome response with assigned UID and ClientNick
		resp, _ := json.Marshal(TextMsgOut{
			Type:       "auth_success",
			SenderUID:  c.uid,
			Sender:     c.username,
			ClientNick: c.clientNick,
			ClientUID:  int64(c.uid),
			GameIGN:    c.gameIGN,
			Timestamp:  time.Now().UnixMilli(),
		})
		c.sendText <- resp

		log.Printf("[Auth] Клиент: %s (UID: %d, IGN: %s, HWID: %s, Server: %s)", c.clientNick, c.uid, c.gameIGN, in.HWID, in.ServerIP)
		h.broadcastOnline()
		h.syncIgnDirectory(c)

		// Alert users on the same Minecraft server
		if in.ServerIP != "" {
			h.alertServerUsers(c, in.ServerIP)
		}

	case "ign_sync":
		if in.GameIGN == "" {
			return
		}
		c.mu.Lock()
		c.gameIGN = in.GameIGN
		if c.clientNick == "" {
			c.clientNick = c.username
		}
		cNick := c.clientNick
		cUID := int64(c.uid)
		sIP := c.serverIP
		c.mu.Unlock()

		log.Printf("[IGNSync] Юзер %s (UID: %d) теперь играет как: %s", cNick, cUID, in.GameIGN)
		msg, _ := json.Marshal(TextMsgOut{
			Type:       "ign_update",
			ClientNick: cNick,
			ClientUID:  cUID,
			GameIGN:    in.GameIGN,
			Message:    sIP,
			Timestamp:  time.Now().UnixMilli(),
		})
		h.broadcastAllText(msg)

	case "server_change":
		c.mu.Lock()
		c.serverIP = in.ServerIP
		cNick := c.clientNick
		cUID := int64(c.uid)
		gIGN := c.gameIGN
		c.mu.Unlock()
		if cNick != "" && gIGN != "" {
			msg, _ := json.Marshal(TextMsgOut{
				Type:       "ign_update",
				ClientNick: cNick,
				ClientUID:  cUID,
				GameIGN:    gIGN,
				Message:    in.ServerIP,
				Timestamp:  time.Now().UnixMilli(),
			})
			h.broadcastAllText(msg)
		}
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
	sender.mu.RLock()
	sNick := sender.clientNick
	sIGN := sender.gameIGN
	sServer := sender.serverIP
	sParty := sender.partyID
	sUID := sender.uid
	sUsername := sender.username
	sender.mu.RUnlock()

	out := TextMsgOut{
		Type:       "chat",
		Timestamp:  time.Now().UnixMilli(),
		SenderUID:  sUID,
		Sender:     sUsername,
		ClientNick: sNick,
		ClientUID:  int64(sUID),
		GameIGN:    sIGN,
		Channel:    in.Channel,
		Target:     in.Target,
		Message:    in.Message,
	}
	bytes, _ := json.Marshal(out)

	senderServer := sServer
	senderParty := sParty

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
			if c.authorized && (c == sender || strings.EqualFold(c.clientNick, in.Target) || strings.EqualFold(c.gameIGN, in.Target) || strings.EqualFold(c.username, in.Target)) {
				c.sendText <- bytes
			}
		}
	}
}

type IgnEntry struct {
	ClientNick string `json:"client_nick"`
	ClientUID  int64  `json:"client_uid"`
	GameIGN    string `json:"game_ign"`
	ServerIP   string `json:"server_ip"`
}

func (h *Hub) syncIgnDirectory(newUser *Client) {
	newUser.mu.RLock()
	newNick := newUser.clientNick
	newUID := int64(newUser.uid)
	newIGN := newUser.gameIGN
	newSrv := newUser.serverIP
	newUser.mu.RUnlock()

	if newNick != "" {
		updateMsg, _ := json.Marshal(TextMsgOut{
			Type:       "ign_update",
			ClientNick: newNick,
			ClientUID:  newUID,
			GameIGN:    newIGN,
			Message:    newSrv,
			Timestamp:  time.Now().UnixMilli(),
		})
		h.clientsMu.RLock()
		for c := range h.clients {
			if c != newUser && c.authorized {
				select {
				case c.sendText <- updateMsg:
				default:
				}
			}
		}
		h.clientsMu.RUnlock()
	}

	h.clientsMu.RLock()
	entries := make([]IgnEntry, 0, len(h.clients))
	for c := range h.clients {
		if c.authorized {
			c.mu.RLock()
			if c.clientNick != "" {
				entries = append(entries, IgnEntry{
					ClientNick: c.clientNick,
					ClientUID:  int64(c.uid),
					GameIGN:    c.gameIGN,
					ServerIP:   c.serverIP,
				})
			}
			c.mu.RUnlock()
		}
	}
	h.clientsMu.RUnlock()

	snapMsg, _ := json.Marshal(TextMsgOut{
		Type:      "ign_snapshot",
		Data:      entries,
		Timestamp: time.Now().UnixMilli(),
	})
	select {
	case newUser.sendText <- snapMsg:
	default:
	}
}

func (h *Hub) alertServerUsers(newUser *Client, serverIP string) {
	h.clientsMu.RLock()
	defer h.clientsMu.RUnlock()

	for c := range h.clients {
		if c != newUser && c.authorized && c.serverIP == serverIP {
			alert, _ := json.Marshal(TextMsgOut{
				Type:       "user_alert",
				Message:    "Юзер AevumDLC " + newUser.clientNick + " играет на твоем сервере!",
				Sender:     newUser.clientNick,
				ClientNick: newUser.clientNick,
				GameIGN:    newUser.gameIGN,
				Timestamp:  time.Now().UnixMilli(),
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
	case "invite":
		targetName := in.Target
		senderName := c.username
		if c.partyID == "" {
			c.partyID = senderName
		}
		senderParty := c.partyID
		c.mu.Unlock()

		h.clientsMu.RLock()
		for targetClient := range h.clients {
			if targetClient.authorized && (strings.EqualFold(targetClient.clientNick, targetName) || strings.EqualFold(targetClient.gameIGN, targetName) || strings.EqualFold(targetClient.username, targetName)) {
				inviteMsg, _ := json.Marshal(TextMsgOut{
					Type:      "party_invite",
					Sender:    senderName,
					Channel:   senderParty,
					Timestamp: time.Now().UnixMilli(),
				})
				select {
				case targetClient.sendText <- inviteMsg:
				default:
				}
				break
			}
		}
		h.clientsMu.RUnlock()
		return
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
	ClientNick string  `json:"client_nick"`
	GameIGN    string  `json:"game_ign"`
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
				ClientNick: m.clientNick,
				GameIGN:    m.gameIGN,
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
