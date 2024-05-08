SHELL = /bin/bash

.PHONE: run test

run:
	$(env) go run cmd/main.go

test:
	go test -race $$(go list ./... | grep -vE '/vendor/') -count=1 -cover