package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

var (
	nickRegex = regexp.MustCompile(`^[a-zA-Z0-9_]{3,16}$`)

	reservedNicks = map[string]bool{
		"admin":     true,
		"administrator": true,
		"system":    true,
		"server":    true,
		"moderator": true,
		"mod":       true,
		"owner":     true,
		"root":      true,
		"dobby":     true,
		"vanta":     true,
		"aevum":     true,
		"aevumdlc":  true,
		"null":      true,
	}
)

type UserRecord struct {
	UID       int64  `json:"uid"`
	Nick      string `json:"nick"`
	HWID      string `json:"hwid"`
	CreatedAt int64  `json:"created_at"`
}

type RegistryData struct {
	LastUID int64                 `json:"last_uid"`
	Users   map[string]UserRecord `json:"users"` // key: lowercase nick
	ByHWID  map[string]string     `json:"by_hwid"` // key: hwid -> lowercase nick
}

type Registry struct {
	filePath string
	mu       sync.RWMutex
	data     RegistryData
}

func NewRegistry(filePath string) *Registry {
	r := &Registry{
		filePath: filePath,
		data: RegistryData{
			LastUID: 0,
			Users:   make(map[string]UserRecord),
			ByHWID:  make(map[string]string),
		},
	}
	r.load()
	return r
}

func (r *Registry) load() {
	r.mu.Lock()
	defer r.mu.Unlock()

	raw, err := os.ReadFile(r.filePath)
	if err != nil {
		return
	}

	var d RegistryData
	if err := json.Unmarshal(raw, &d); err != nil {
		return
	}

	if d.Users == nil {
		d.Users = make(map[string]UserRecord)
	}
	if d.ByHWID == nil {
		d.ByHWID = make(map[string]string)
	}

	r.data = d
}

func (r *Registry) saveLocked() error {
	dir := filepath.Dir(r.filePath)
	if dir != "" && dir != "." {
		_ = os.MkdirAll(dir, 0755)
	}

	raw, err := json.MarshalIndent(r.data, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(r.filePath, raw, 0644)
}

func (r *Registry) ClaimNick(nick string, hwid string) (int64, string, error) {
	trimmedNick := strings.TrimSpace(nick)
	trimmedHWID := strings.TrimSpace(hwid)

	if !nickRegex.MatchString(trimmedNick) {
		return 0, "", errors.New("Никнейм должен быть от 3 до 16 символов (латиница, цифры и _)")
	}

	lowerNick := strings.ToLower(trimmedNick)
	if reservedNicks[lowerNick] {
		return 0, "", errors.New("Этот никнейм зарезервирован системой")
	}

	if trimmedHWID == "" {
		trimmedHWID = "unknown"
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// 1. Check if HWID already has a registered nickname
	if existingLowerNick, ok := r.data.ByHWID[trimmedHWID]; ok {
		if rec, found := r.data.Users[existingLowerNick]; found {
			// If trying to claim the same nickname they already own, return success
			if strings.EqualFold(existingLowerNick, lowerNick) {
				return rec.UID, rec.Nick, nil
			}
			// If device is already registered to a different nickname
			return rec.UID, rec.Nick, errors.New("За вашим устройством уже закреплен никнейм: " + rec.Nick)
		}
	}

	// 2. Check if nickname is taken by someone else
	if existingUser, exists := r.data.Users[lowerNick]; exists {
		if existingUser.HWID == trimmedHWID {
			return existingUser.UID, existingUser.Nick, nil
		}
		return 0, "", errors.New("Этот никнейм уже занят другим пользователем")
	}

	// 3. Increment UID (1, 2, 3...)
	r.data.LastUID++
	newUID := r.data.LastUID

	record := UserRecord{
		UID:       newUID,
		Nick:      trimmedNick,
		HWID:      trimmedHWID,
		CreatedAt: time.Now().Unix(),
	}

	r.data.Users[lowerNick] = record
	r.data.ByHWID[trimmedHWID] = lowerNick

	_ = r.saveLocked()
	return newUID, trimmedNick, nil
}

func (r *Registry) GetByNick(nick string) (UserRecord, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	rec, ok := r.data.Users[strings.ToLower(strings.TrimSpace(nick))]
	return rec, ok
}

func (r *Registry) GetByHWID(hwid string) (UserRecord, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	lowerNick, ok := r.data.ByHWID[strings.TrimSpace(hwid)]
	if !ok {
		return UserRecord{}, false
	}
	rec, found := r.data.Users[lowerNick]
	return rec, found
}
