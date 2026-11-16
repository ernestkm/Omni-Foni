FROM golang:1.27 AS build
WORKDIR /src
COPY observability/ ./observability/
COPY charging/ ./charging/
COPY mediation/ ./mediation/
COPY routing/ ./routing/
COPY sbc/ ./sbc/
COPY switching/ ./switching/
RUN cd switching && CGO_ENABLED=0 go build -o /switching .

FROM gcr.io/distroless/static:nonroot
COPY --from=build /switching /switching
ENTRYPOINT ["/switching"]
