.PHONY run client test
run:
	go run ./cmd/master -addr :8888 -dir ./master_data -replication 3
	go run ./cmd/chunkserver -addr :8901 -master :8888 -dir ./cs_data1 -rack rack0
	go run ./cmd/chunkserver -addr :8902 -master :8888 -dir ./cs_data2 -rack rack1
	go run ./cmd/chunkserver -addr :8903 -master :8888 -dir ./cs_data3 -rack rack2
client:
	go run ./cmd/client -master :8888 -path /demo.bin -size-mb 16
test:
	go test ./internal/client/ -count=1
	go test ./internal/client/ -run TestBasicReadWrite      # 基础读写(100MB)
	go test ./internal/client/ -run TestConcurrentAppend    # 并发原子追加(10 goroutine)
	go test ./internal/client/ -run TestChunkServerFailure  # ChunkServer 故障与副本再复制
	go test ./internal/client/ -run TestMasterRestart       # Master 重启(心跳重建位置)
	go test ./internal/client/ -run TestSnapshot            # 快照(写时复制)
	go test ./internal/client/ -count=1 -race
