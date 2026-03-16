# ---- build stage ----
FROM golang:1.24-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /javi-collector ./cmd/collector

# ---- runtime stage ----
FROM alpine:3.20

RUN apk --no-cache add ca-certificates tzdata

COPY --from=builder /javi-collector /javi-collector

EXPOSE 4317 4318

ENTRYPOINT ["/javi-collector"]
