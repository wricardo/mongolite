BINARY     := mongolitesrv
CLI_BINARY := mongolite
MODULE     := github.com/wricardo/mongolite

.PHONY: build build-cli install install-cli install-skill test lint clean

build:
	go build -o $(BINARY) .

build-cli:
	go build -o $(CLI_BINARY) ./cmd/mongolite

install:
	go install $(MODULE)

install-cli:
	go install $(MODULE)/cmd/mongolite

install-skill: install
	$(BINARY) install-skill

test:
	go test ./...

lint:
	go vet ./...

clean:
	rm -f $(BINARY) $(CLI_BINARY)
