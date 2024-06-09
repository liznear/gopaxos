build-proto:
    protoc --go_out=. --go_opt=paths=source_relative \
        --go-grpc_out=. --go-grpc_opt=paths=source_relative \
        proto/*.proto

test:
    go test -v ./...

run1:
    go run example/kv.go -port 8081 -id 1 -debug -peers "1=localhost:8081,2=localhost:8082,3=localhost:8083"

run2:
    go run example/kv.go -port 8082 -id 2 -debug -peers "1=localhost:8081,2=localhost:8082,3=localhost:8083"

run3:
    go run example/kv.go -port 8083 -id 3 -debug -peers "1=localhost:8081,2=localhost:8082,3=localhost:8083"
