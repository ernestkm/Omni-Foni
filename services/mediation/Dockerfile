FROM golang:1.27 AS build
WORKDIR /src
COPY observability/ ./observability/
COPY mediation/ ./mediation/
RUN cd mediation && CGO_ENABLED=0 go build -o /mediation .

FROM gcr.io/distroless/static
COPY --from=build /mediation /mediation
ENTRYPOINT ["/mediation"]
