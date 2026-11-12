FROM golang:1.27 AS build
WORKDIR /src
COPY catalogue/ ./catalogue/
COPY charging/ ./charging/
RUN cd charging && CGO_ENABLED=0 go build -o /charging .

FROM gcr.io/distroless/static
COPY --from=build /charging /charging
ENTRYPOINT ["/charging"]
