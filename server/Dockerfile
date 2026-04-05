# Build
FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /horizon-server ./cmd/server

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=build /horizon-server /usr/local/bin/horizon-server
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/horizon-server"]
