package hooks

import (
	"encoding/json"

	"github.com/golang/protobuf/proto"
)

/*
EncodeHooks encodes a list of WebHooks as a byte array in protobuf format.
*/
func EncodeHooks(hooks []WebHook) []byte {
	cfg := WebHookConfigPb{}

	for _, hook := range hooks {
		pb := WebHookPb{
			Url: proto.String(hook.URI),
		}
		for key, val := range hook.Headers {
			hdrPb := HeaderPb{
				Name:  proto.String(key),
				Value: proto.String(val),
			}
			pb.Headers = append(pb.Headers, &hdrPb)
		}
		cfg.Hooks = append(cfg.Hooks, &pb)
	}

	buf, err := proto.Marshal(&cfg)
	if err != nil {
		panic(err.Error())
	}
	return buf
}

/*
EncodeHooksJSON encodes a list of WebHooks as JSON.
*/
func EncodeHooksJSON(hooks []WebHook) []byte {
	buf, err := json.Marshal(hooks)
	if err != nil {
		panic(err.Error())
	}
	return buf
}

/*
DecodeHooks turns the input of EncodeHooks into a list of WebHook objects.
*/
func DecodeHooks(buf []byte) ([]WebHook, error) {
	var cfg WebHookConfigPb
	err := proto.Unmarshal(buf, &cfg)
	if err != nil {
		return nil, err
	}

	var hooks []WebHook
	for _, hookPb := range cfg.GetHooks() {
		newHook := WebHook{
			URI:     hookPb.GetUrl(),
			Headers: make(map[string]string),
		}
		for _, hdrPb := range hookPb.Headers {
			newHook.Headers[hdrPb.GetName()] = hdrPb.GetValue()
		}
		hooks = append(hooks, newHook)
	}
	return hooks, nil
}

/*
DecodeHooksJSON turns the JSON representation of the hooks into a list
of WebHook objects.
*/
func DecodeHooksJSON(buf []byte) ([]WebHook, error) {
	var hooks []WebHook
	err := json.Unmarshal(buf, &hooks)
	if err != nil {
		return nil, err
	}
	return hooks, nil
}
