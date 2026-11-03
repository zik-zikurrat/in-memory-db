.PHONY: build test lint clean vet race

build:
	go build ./...

test:
	go test ./...

race:
	go test -race ./...

lint:
	golangci-lint run

clean:
	rm -rf bin/
