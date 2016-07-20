package auth

import (
	"bytes"
	"crypto/sha256"
	"sync"
)

type authUser struct {
	encoded string
	cache   []byte
}

/*
A Store is a thread-safe collection of usernames and passwords.
*/
type Store struct {
	users  map[string]*authUser
	latch  *sync.RWMutex
	pwFile string
	stop   chan bool
}

/*
NewAuthStore creates a new, empty Store.
*/
func NewAuthStore() *Store {
	return &Store{
		users: make(map[string]*authUser),
		latch: &sync.RWMutex{},
		stop:  make(chan bool, 1),
	}
}

/*
IsEmpty returns true if there is nothing in the store. We might use this to
determine if we should skip authentication.
*/
func (a *Store) IsEmpty() bool {
	a.latch.RLock()
	defer a.latch.RUnlock()
	return len(a.users) == 0
}

/*
Close releases any resources used by the store. It is important to call this
if "Watch" was used.
*/
func (a *Store) Close() {
	a.stop <- true
}

/*
Authenticate returns true if the specified username and password are part
of the store.
*/
func (a *Store) Authenticate(user, pass string) bool {
	var encoded string
	var cache []byte

	a.latch.RLock()
	pw := a.users[user]
	if pw != nil {
		encoded = pw.encoded
		cache = pw.cache
	}
	a.latch.RUnlock()

	if encoded == "" {
		return false
	}

	pwb := []byte(pass)
	if cache == nil {
		ok := matchEncoded(pwb, encoded)
		if ok {
			// Cache encoded password in a faster way but still hashed
			encodedFast := encodeFast(pwb)
			a.latch.Lock()
			pw = a.users[user]
			if pw != nil {
				pw.cache = encodedFast
			}
			a.latch.Unlock()
		}
		return ok
	}

	encodedFast := encodeFast(pwb)
	return bytes.Equal(cache, encodedFast)
}

func encodeFast(pass []byte) []byte {
	enc := sha256.New()
	return enc.Sum(pass)
}
