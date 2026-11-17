FROM golang:1.27 AS build
WORKDIR /src
COPY observability/ ./observability/
COPY catalogue/ ./catalogue/
COPY mediation/ ./mediation/
COPY reports/ ./reports/
RUN cd reports && CGO_ENABLED=0 go build -o /reports .

FROM gcr.io/distroless/static
COPY --from=build /reports /reports
ENTRYPOINT ["/reports"]
