# Multi-stage build: compile a static binary, ship it in an empty image.
FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/streamhub ./cmd/streamhub

FROM scratch
COPY --from=build /out/streamhub /streamhub
EXPOSE 9092 8080
VOLUME ["/data"]
ENTRYPOINT ["/streamhub"]
CMD ["broker", "--listen", "0.0.0.0:9092", "--http", "0.0.0.0:8080", "--data-dir", "/data"]
