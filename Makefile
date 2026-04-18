BINARY := mongolite
MODULE := github.com/wricardo/mongolite

.PHONY: build install install-skill test lint clean

build:
	go build -o $(BINARY) ./cmd/mongolite

install:
	go install $(MODULE)/cmd/mongolite

install-skill: install
	$(BINARY) install-skill

test:
	go test ./...

lint:
	go vet ./...

clean:
	rm -f $(BINARY)
