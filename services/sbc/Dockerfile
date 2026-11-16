FROM golang:1.27 AS build
WORKDIR /src
COPY observability/ ./observability/
COPY sbc/ ./sbc/
RUN cd sbc && CGO_ENABLED=0 go build -o /sbc .

FROM gcr.io/distroless/static
COPY --from=build /sbc /sbc
ENTRYPOINT ["/sbc"]
