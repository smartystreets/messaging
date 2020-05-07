#!/usr/bin/make -f

test: fmt
	go test -count=1 -timeout=1s -short -race -covermode=atomic ./...

testall:
	go test -count=1 ./...

fmt:
	go fmt ./...

compile:
	go build ./...

build: test compile

.PHONY: test testall fmt compile build
