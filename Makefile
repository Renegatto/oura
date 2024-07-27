.PHONY=run
writable := /home/alexey/oura/our-daemon.socket
coordination := /home/alexey/oura/our-daemon-tx.socket

clear_sockets:
	@rm ${writable} ${coordination} || true

listen:
	@rm ${coordination} || true
	@cargo run -- socket \
	  --writable-socket ${writable} \
	  --coordination-socket ${coordination}

run:
	@rm ${writable} || true
	@cargo run -- daemon --config daemon.toml