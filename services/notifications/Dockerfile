FROM golang:1.27 AS build
WORKDIR /src
COPY observability/ /src/observability/
COPY notifications/ /src/notifications/
WORKDIR /src/notifications
RUN CGO_ENABLED=0 go build -trimpath -o /out/notifications .

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=build /out/notifications /app/notifications
COPY --from=build /src/notifications/migrate.sql /app/migrate.sql
USER nonroot:nonroot
ENTRYPOINT ["/app/notifications"]
