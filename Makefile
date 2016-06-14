ALLDEPS = \
  agent/*.go communication/*.go \
	discovery/*.go storage/*.go raft/*.go agent/*.go \
	hooks/*.go

all: agent

agent: agent/agent

agent/agent: $(ALLDEPS)
	(cd agent; go build)

test: agent
	go test -v `glide nv`

clean:
	(cd agent; go clean)
	rm -f agent/agent
