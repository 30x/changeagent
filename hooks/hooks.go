package hooks

//go:generate protoc --go_out=. webhookconfig.proto

/*
This module will contain a few mechanisms that let us manage webhooks.
It handles the definitions for registering and defining webhooks, and it
calls them using HTTP(s). Other modules will handle storing the definitions.
*/

/*
A Header is simply a key-value pair. Each web hook can optionally specify
a set of headers that it will send on each request.
*/
type Header struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

/*
A WebHook defines a single hook. The URL describes what we will call.
A WebHook will be implemented as an HTTP (or HTTPS) POST to the specified
URI, with content type and content defined by the user of this package.
The target must return a status code in the "200" to indicate that the hook
succeeded. Anything else will be interpreted as an error.
*/
type WebHook struct {
	URI     string            `json:"uri"`
	Headers map[string]string `json:"headers,omitempty"`
}
