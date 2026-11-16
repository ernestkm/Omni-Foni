FROM golang:1.27 AS build
WORKDIR /src
COPY observability/ ./observability/
COPY routing/ ./routing/
RUN cd routing && CGO_ENABLED=0 go build -o /routing .

FROM gcr.io/distroless/static
COPY --from=build /routing /routing
ENTRYPOINT ["/routing"]
