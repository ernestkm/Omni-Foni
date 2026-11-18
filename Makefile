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
	cd services/billing && go vet ./... && go test ./...
	cd services/catalogue && go vet ./... && go test ./...
	cd services/charging && go vet ./... && go test ./...
	cd services/fraud && go vet ./... && go test ./...
	cd services/identity && go vet ./... && go test ./...
	cd services/mediation && go vet ./... && go test ./...
	cd services/observability && go vet ./... && go test ./...
	cd services/payments && go vet ./... && go test ./...
	cd services/reconciliation && go vet ./... && go test ./...
	cd services/reports && go vet ./... && go test ./...
	cd services/routing && go vet ./... && go test ./...
	cd services/sbc && go vet ./... && go test ./...
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
	PATH=/root/go/bin:$(PATH) protoc -I proto --go_out=services/mediation --go_opt=module=omnifoni/mediation \
		proto/mediation/v1/mediation.proto
	PATH=/root/go/bin:$(PATH) protoc -I proto --go_out=services/billing --go_opt=module=omnifoni/billing \
		--go-grpc_out=services/billing --go-grpc_opt=module=omnifoni/billing \
		proto/billing/v1/billing.proto
	PATH=/root/go/bin:$(PATH) protoc -I proto --go_out=services/routing --go_opt=module=omnifoni/routing \
		--go-grpc_out=services/routing --go-grpc_opt=module=omnifoni/routing \
		proto/routing/v1/routing.proto
	PATH=/root/go/bin:$(PATH) protoc -I proto --go_out=services/sbc --go_opt=module=omnifoni/sbc \
		--go-grpc_out=services/sbc --go-grpc_opt=module=omnifoni/sbc \
		proto/sbc/v1/sbc.proto
	PATH=/root/go/bin:$(PATH) protoc -I proto --go_out=services/payments --go_opt=module=omnifoni/payments \
		--go-grpc_out=services/payments --go-grpc_opt=module=omnifoni/payments \
		proto/payments/v1/payments.proto
	PATH=/root/go/bin:$(PATH) protoc -I proto --go_out=services/reconciliation --go_opt=module=omnifoni/reconciliation \
		--go-grpc_out=services/reconciliation --go-grpc_opt=module=omnifoni/reconciliation \
		proto/reconciliation/v1/reconciliation.proto
	PATH=/root/go/bin:$(PATH) protoc -I proto --go_out=services/reports --go_opt=module=omnifoni/reports \
		--go-grpc_out=services/reports --go-grpc_opt=module=omnifoni/reports \
		proto/reports/v1/reports.proto
	PATH=/root/go/bin:$(PATH) protoc -I proto --go_out=services/reports --go_opt=module=omnifoni/reports \
		proto/scheduling/v1/scheduling.proto
	PATH=/root/go/bin:$(PATH) protoc -I proto --go_out=services/fraud --go_opt=module=omnifoni/fraud \
		--go-grpc_out=services/fraud --go-grpc_opt=module=omnifoni/fraud \
		proto/fraud/v1/fraud.proto
