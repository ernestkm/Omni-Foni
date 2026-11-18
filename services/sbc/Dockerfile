FROM golang:1.27 AS build
WORKDIR /src
COPY observability/ ./observability/
COPY fraud/ ./fraud/
COPY sbc/ ./sbc/
RUN cd sbc && CGO_ENABLED=0 go build -o /sbc .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /sbc /sbc
EXPOSE 50058
USER nonroot:nonroot
ENTRYPOINT ["/sbc"]
