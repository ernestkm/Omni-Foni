module omnifoni/switching

go 1.27.4

replace omnifoni/mediation => ../mediation

replace omnifoni/charging => ../charging

replace omnifoni/catalogue => ../catalogue

replace omnifoni/routing => ../routing

replace omnifoni/sbc => ../sbc

require (
	github.com/google/uuid v1.6.0
	github.com/jackc/pgx/v5 v5.13.0
	github.com/twmb/franz-go v1.22.3
	github.com/twmb/franz-go/pkg/kadm v1.20.0
	google.golang.org/grpc v1.85.0
	google.golang.org/protobuf v1.36.12
	omnifoni/catalogue v0.0.0-00010101000000-000000000000
	omnifoni/charging v0.0.0-00010101000000-000000000000
	omnifoni/mediation v0.0.0-00010101000000-000000000000
	omnifoni/routing v0.0.0-00010101000000-000000000000
	omnifoni/sbc v0.0.0-00010101000000-000000000000
)

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.3 // indirect
	github.com/klauspost/compress v1.20.3 // indirect
	github.com/pierrec/lz4/v4 v4.1.33 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.15.1 // indirect
	golang.org/x/net v0.60.0 // indirect
	golang.org/x/sync v0.23.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
	golang.org/x/text v0.42.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260817212433-ac3dfec99bb1 // indirect
)

replace omnifoni/observability => ../observability
