build:
	cargo build --release -p processor

install:
	 cp target/release/processor ../bin/
