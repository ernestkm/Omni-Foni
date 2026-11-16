FROM golang:1.27 AS build
WORKDIR /src
COPY observability/ ./observability/
COPY billing/ ./billing/
COPY payments/ ./payments/
COPY reconciliation/ ./reconciliation/
RUN cd reconciliation && CGO_ENABLED=0 go build -o /reconciliation .

FROM gcr.io/distroless/static:nonroot
COPY --from=build /reconciliation /reconciliation
ENTRYPOINT ["/reconciliation"]