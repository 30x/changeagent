package auth

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"regexp"
	"strings"
	"sync"

	"golang.org/x/crypto/pbkdf2"
)

const (
	keyLen     = 32
	saltLen    = 4
	iterations = 10000
)

var basicRe = regexp.MustCompile("^Basic[\\s]+(.+)")

type authUser struct {
	salt []byte
	pass []byte
	fast []byte
}

/*
A Store is a thread-safe collection of usernames and passwords.
*/
type Store struct {
	users map[string]*authUser
	latch *sync.RWMutex
}

/*
NewAuthStore creates a new, empty Store.
*/
func NewAuthStore() *Store {
	return &Store{
		users: make(map[string]*authUser),
		latch: &sync.RWMutex{},
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
SetUser inserts a username and password.
*/
func (a *Store) SetUser(user, pass string) {
	salt := makeSalt()
	enc := encodePass([]byte(pass), salt)

	a.latch.Lock()
	a.users[user] = &authUser{
		salt: salt,
		pass: enc,
	}
	a.latch.Unlock()
}

/*
DeleteUser removes a user.
*/
func (a *Store) DeleteUser(user string) {
	a.latch.Lock()
	delete(a.users, user)
	a.latch.Unlock()
}

/*
Authenticate returns true if the specified username and password are part
of the store.
*/
func (a *Store) Authenticate(user, pass string) bool {
	var encoded, encodedFast []byte
	a.latch.RLock()
	pw := a.users[user]
	if pw != nil {
		encoded = pw.pass
		encodedFast = pw.fast
	}
	a.latch.RUnlock()

	if encoded == nil {
		return false
	}

	pwb := []byte(pass)
	if encodedFast == nil {
		encodedIn := encodePass(pwb, pw.salt)
		ok := bytes.Equal(encodedIn, encoded)
		if ok {
			encodedFast = encodeFast(pwb)
			a.latch.Lock()
			pw = a.users[user]
			if pw != nil {
				pw.fast = encodedFast
			}
			a.latch.Unlock()
		}
		return ok
	}

	fast := encodeFast(pwb)
	return bytes.Equal(fast, encodedFast)
}

/*
AuthenticateBasic takes an "Authorization" header and returns true if
it is a valid "basic" style header and if it is valid.
*/
func (a *Store) AuthenticateBasic(hdr string) bool {
	match := basicRe.FindStringSubmatch(hdr)
	if match == nil {
		return false
	}
	decBytes, err := base64.StdEncoding.DecodeString(match[1])
	if err != nil {
		return false
	}
	userpass := strings.SplitN(string(decBytes), ":", 2)
	if len(userpass) < 2 {
		return false
	}

	return a.Authenticate(userpass[0], userpass[1])
}

func encodePass(pass, salt []byte) []byte {
	return pbkdf2.Key(pass, salt, iterations, keyLen, sha256.New)
}

func encodeFast(pass []byte) []byte {
	enc := sha256.New()
	return enc.Sum(pass)
}

func makeSalt() []byte {
	buf := make([]byte, saltLen)
	_, err := rand.Read(buf)
	if err != nil {
		panic(err.Error())
	}
	return buf
}
