BINARY := mongolite
MODULE := github.com/wricardo/mongolite

.PHONY: build install install-skill test lint clean

build:
	go build -o $(BINARY) .

install:
	go install $(MODULE)

install-skill: install
	$(BINARY) install-skill

test:
	go test ./...

lint:
	go vet ./...

clean:
	rm -f $(BINARY)
