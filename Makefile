ALLDEPS = \
  agent/*.go log/*.go communication/*.go \
	discovery/*.go storage/*.go raft/*.go agent/*.go

all: agent

agent: agent/agent

agent/agent: $(ALLDEPS)
	(cd agent; go build)

test:
	(cd log; go test)
	(cd communication; go test)
	(cd discovery; go test)
	(cd storage; go test)
	(cd raft; go test)
	(cd agent; go test)

clean:
	(cd agent; go clean)
	rm -f agent/agent
