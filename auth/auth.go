package auth

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"sync"

	"github.com/30x/changeagent/protobufs"
	"github.com/golang/protobuf/proto"
	"golang.org/x/crypto/pbkdf2"
)

const (
	keyLen     = 32
	saltLen    = 4
	iterations = 10000
)

type authUser struct {
	salt []byte
	pass []byte
	fast []byte
}

type AuthStore struct {
	users map[string]*authUser
	latch *sync.RWMutex
}

func NewAuthStore() *AuthStore {
	return &AuthStore{
		users: make(map[string]*authUser),
		latch: &sync.RWMutex{},
	}
}

func (a *AuthStore) SetUser(user, pass string) {
	salt := makeSalt()
	enc := encodePass([]byte(pass), salt)

	a.latch.Lock()
	a.users[user] = &authUser{
		salt: salt,
		pass: enc,
	}
	a.latch.Unlock()
}

func (a *AuthStore) DeleteUser(user string) {
	a.latch.Lock()
	delete(a.users, user)
	a.latch.Unlock()
}

func DecodeAuthStore(buf []byte) (*AuthStore, error) {
	var table protobufs.UserTablePb
	err := proto.Unmarshal(buf, &table)
	if err != nil {
		return nil, err
	}

	as := NewAuthStore()

	for _, u := range table.GetUsers() {
		au := authUser{
			salt: u.GetSalt(),
			pass: u.GetPassword(),
		}
		as.users[u.GetUser()] = &au
	}

	return as, nil
}

func (a *AuthStore) Encode() []byte {
	a.latch.RLock()
	defer a.latch.RUnlock()

	var users []*protobufs.UserPb
	for user, u := range a.users {
		pb := protobufs.UserPb{
			User:     proto.String(user),
			Password: u.pass,
			Salt:     u.salt,
		}
		users = append(users, &pb)
	}
	table := protobufs.UserTablePb{
		Users: users,
	}
	buf, err := proto.Marshal(&table)
	if err != nil {
		panic(err.Error())
	}
	return buf
}

func (a *AuthStore) Authenticate(user, pass string) bool {
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
