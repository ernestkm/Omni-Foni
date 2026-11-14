FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /bin/sbc .

FROM gcr.io/distroless/static
COPY --from=build /bin/sbc /sbc
ENTRYPOINT ["/sbc"]
