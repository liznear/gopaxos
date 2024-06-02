build-proto:
    protoc --go_out=. --go_opt=paths=source_relative \
        --go-grpc_out=. --go-grpc_opt=paths=source_relative \
        core/proto/*.proto

run1:
    go run example/kv.go -port 8081 -id 1 -debug -peers "1=localhost:8080,2=localhost:8081,3=localhost:8082"

run2:
    go run example/kv.go -port 8082 -id 2 -debug -peers "1=localhost:8080,2=localhost:8081,3=localhost:8082"

run3:
    go run example/kv.go -port 8083 -id 3 -debug -peers "1=localhost:8080,2=localhost:8081,3=localhost:8082"
