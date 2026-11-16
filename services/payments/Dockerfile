FROM golang:1.27 AS build
WORKDIR /src
COPY observability/ ./observability/
COPY payments/ ./payments/
RUN cd payments && CGO_ENABLED=0 go build -o /payments .

FROM gcr.io/distroless/static
COPY --from=build /payments /payments
ENTRYPOINT ["/payments"]
