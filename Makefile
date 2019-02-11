# Set if your compiler cannot find liblzo2
# LIBRARY_PATH = /path/to/liblzo2

DESTDIR := dist

all: release

release: target/release/backy-extract

target/release/backy-extract: src/*.rs src/*/*.rs
	cargo build --release
	strip $@

VERSION = `cargo read-manifest | jq .version -r`
PV = backy-extract-$(VERSION)

dist: release
	install -D target/release/backy-extract -t tmp/$(PV)/bin
	install -D -m 0644 README.md -t tmp/$(PV)/share/doc
	mkdir -p dist
	tar czf dist/$(PV).tar.gz -C tmp $(PV)
	rm -r tmp

clean:
	cargo clean
	rm -rf tmp dist

.PHONY: release dist clean
