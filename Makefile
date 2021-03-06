ALLDEPS = \
	agent/*.go communication/*.go \
	storage/*.go raft/*.go agent/*.go \
	hooks/*.go \
	glide.lock

all: changeagent

changeagent: $(ALLDEPS)
	go build -o $@ ./agent

test: changeagent
	go test -v `glide nv`

clean:
	rm -f changeagent
