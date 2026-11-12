.PHONY: up down ps smoke logs test protoc

up:
	docker compose up -d --wait

down:
	docker compose down

ps:
	docker compose ps

logs:
	docker compose logs -f

smoke:
	bash ops/smoke.sh

test:
	cd services/catalogue && go vet ./... && go test ./...
	cd services/charging && go vet ./... && go test ./...
	cd services/switching && go vet ./... && go test ./...

protoc:
	PATH=/root/go/bin:$(PATH) protoc -I proto --go_out=services/catalogue --go_opt=module=omnifoni/catalogue \
		--go-grpc_out=services/catalogue --go-grpc_opt=module=omnifoni/catalogue \
		proto/catalogue/v1/catalogue.proto
	PATH=/root/go/bin:$(PATH) protoc -I proto --go_out=services/charging --go_opt=module=omnifoni/charging \
		--go-grpc_out=services/charging --go-grpc_opt=module=omnifoni/charging \
		proto/charging/v1/charging.proto
	PATH=/root/go/bin:$(PATH) protoc -I proto --go_out=services/switching --go_opt=module=omnifoni/switching \
		--go-grpc_out=services/switching --go-grpc_opt=module=omnifoni/switching \
		proto/switching/v1/switching.proto
