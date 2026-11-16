FROM golang:1.27 AS build
WORKDIR /src
COPY observability/ ./observability/
COPY billing/ ./billing/
COPY identity/ ./identity/
RUN cd identity && CGO_ENABLED=0 go build -o /identity .

FROM gcr.io/distroless/static
COPY --from=build /identity /identity
ENTRYPOINT ["/identity"]
