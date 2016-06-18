package hooks

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"
)

const (
	httpTimeout = 2 * time.Minute
)

var httpClient = http.Client{
	Timeout: httpTimeout,
}

/*
Invoke invokes all the web hooks stored in the configuration, in parallel.
It will block the calling goroutine. When it has a response from each of
the configured targets, it will return either nil to indicate that the hooks
all succeeded, or an error to indicate that at least one of the hooks failed.
*/
func Invoke(hooks []WebHook, payload []byte, contentType string) error {
	responses := make(chan string, len(hooks))

	for _, hook := range hooks {
		// TODO we could super-optimize for the case of only a single destination
		go invokeOne(hook, payload, contentType, responses)
	}

	errBuf := bytes.Buffer{}
	errCount := 0
	for i := 0; i < len(hooks); i++ {
		resp := <-responses
		if resp != "" {
			errBuf.WriteString(fmt.Sprintf("Error %d: %s\n", errCount, resp))
			errCount++
		}
	}

	if errBuf.Len() > 0 {
		return errors.New(errBuf.String())
	}
	return nil
}

func invokeOne(
	hook WebHook,
	payload []byte,
	contentType string,
	ch chan<- string) {
	uriObj, err := url.Parse(hook.URI)
	if err != nil {
		ch <- err.Error()
		return
	}

	req := http.Request{
		URL:           uriObj,
		Method:        "POST",
		Body:          ioutil.NopCloser(bytes.NewBuffer(payload)),
		ContentLength: int64(len(payload)),
		Header:        make(map[string][]string),
	}
	req.Header.Set("Content-Type", contentType)

	for k, v := range hook.Headers {
		req.Header.Set(k, v)
	}

	resp, err := httpClient.Do(&req)
	if err != nil {
		ch <- err.Error()
		return
	}

	resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		ch <- fmt.Sprintf("WebHook returned status %d", resp.StatusCode)
		return
	}

	ch <- ""
}
