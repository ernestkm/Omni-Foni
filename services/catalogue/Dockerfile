FROM golang:1.27 AS build
WORKDIR /src
COPY observability/ ./observability/
COPY catalogue/ ./catalogue/
RUN cd catalogue && CGO_ENABLED=0 go build -o /catalogue .

FROM gcr.io/distroless/static
COPY --from=build /catalogue /catalogue
ENTRYPOINT ["/catalogue"]
