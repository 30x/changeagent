package auth

import (
	"regexp"

	"golang.org/x/crypto/bcrypt"
)

var bcryptRe = regexp.MustCompile("^\\$2y\\$([0-9]+)\\$(.+)")

func matchEncoded(pw []byte, encoded string) bool {
	match := bcryptRe.FindStringSubmatch(encoded)
	if match == nil {
		// Didn't come from htpasswd bcrypt
		return false
	}

	hash := []byte(encoded)

	err := bcrypt.CompareHashAndPassword(hash, pw)
	return err == nil
}
