ALLDEPS = \
  agent/*.go communication/*.go \
	discovery/*.go storage/*.go raft/*.go agent/*.go \
	hooks/*.go

all: changeagent

changeagent: $(ALLDEPS)
	go build -o $@ ./agent

test: agent
	go test -v `glide nv`

clean:
	rm -f changeagent
