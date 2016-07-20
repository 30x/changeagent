package auth

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"regexp"
	"strings"
)

var basicRe = regexp.MustCompile("^Basic[\\s]+(.+)")

/*
An Handler is an HTTP handler that verifies that incoming requests have
an authenticated user and passes those that pass authentication to
a child.
*/
type Handler struct {
	store        *Store
	childHandler http.Handler
	realm        string
}

/*
CreateHandler creates an HTTP handler for the specified auth store that
will respond with "401 unauthorized" when no authentication is present,
or pass on the request when authentication succeeds.
*/
func (s *Store) CreateHandler(child http.Handler, realm string) *Handler {
	return &Handler{
		store:        s,
		childHandler: child,
		realm:        realm,
	}
}

/*
AuthenticateBasic takes an "Authorization" header and returns true if
it is a valid "basic" style header and if it is valid.
*/
func (s *Store) AuthenticateBasic(hdr string) bool {
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

	return s.Authenticate(userpass[0], userpass[1])
}

func (h *Handler) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	if h.store.AuthenticateBasic(req.Header.Get("Authorization")) {
		h.childHandler.ServeHTTP(resp, req)
	} else {
		resp.Header().Set(
			"WWW-Authenticate",
			fmt.Sprintf("Basic realm=\"%s\"", h.realm))
		resp.WriteHeader(http.StatusUnauthorized)
	}
}
