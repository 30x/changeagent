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
		for _, hdr := range hook.Headers {
			hdrPb := HeaderPb{
				Name:  proto.String(hdr.Name),
				Value: proto.String(hdr.Value),
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
			URI: hookPb.GetUrl(),
		}
		for _, hdrPb := range hookPb.Headers {
			hdr := Header{
				Name:  hdrPb.GetName(),
				Value: hdrPb.GetValue(),
			}
			newHook.Headers = append(newHook.Headers, hdr)
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
