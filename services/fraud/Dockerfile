FROM golang:1.27 AS build
WORKDIR /src
COPY observability/ ./observability/
COPY mediation/ ./mediation/
COPY fraud/ ./fraud/
RUN cd fraud && CGO_ENABLED=0 go build -o /fraud .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /fraud /fraud
EXPOSE 50063
USER nonroot:nonroot
ENTRYPOINT ["/fraud"]
