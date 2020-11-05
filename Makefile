
build_docker_static:
	CGO_CFLAGS="-I/rocksdb/include" CGO_LDFLAGS="-L/rocksdb -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd" go build -tags netgo -ldflags "-w -extldflags \"-static\"" -o core .

run:
	docker build -t slct . && \
	docker run -p 9090:9090 -p 8080:8080 -v $(shell pwd)/db:/db -it slct cp slct /db/slct && \
	sudo chmod +x db/slct && \
	sudo db/slct

proto:
	protoc --go_out=plugins=grpc:. *.proto
	protoc --go_out=plugins=grpc:./bench *.proto
