FROM golang:1.27 AS build
WORKDIR /src
COPY observability/ ./observability/
COPY sipedge/ ./sipedge/
RUN cd sipedge && CGO_ENABLED=0 go build -o /sipedge .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /sipedge /sipedge
USER nonroot:nonroot
ENTRYPOINT ["/sipedge"]
