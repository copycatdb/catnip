.PHONY: build test clean

build:
	cd native && cargo build --release

test: build
	CGO_ENABLED=1 go test -v ./...

clean:
	cd native && cargo clean
