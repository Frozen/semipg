.PHONY: all test build


all: test



test:
	go test -vet=all -race ./...