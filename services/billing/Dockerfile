FROM golang:1.27 AS build
WORKDIR /src
COPY observability/ ./observability/
COPY catalogue/ ./catalogue/
COPY mediation/ ./mediation/
COPY billing/ ./billing/
RUN cd billing && CGO_ENABLED=0 go build -o /billing .

FROM gcr.io/distroless/static
COPY --from=build /billing /billing
ENTRYPOINT ["/billing"]
