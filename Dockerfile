FROM golang:1.25.2 AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go get .

RUN CGO_ENABLED=1 GOOS=linux go build -a -o spotdb .

FROM debian:trixie-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
  ca-certificates \
  libstdc++6 \
  && rm -rf /var/lib/apt/lists/*

RUN groupadd -r appuser && useradd -r -g appuser appuser

WORKDIR /app

COPY --from=builder /app/spotdb .
COPY --from=builder /app/static ./static

ENV ENABLE_QUERY_BENCHMARKS="false"

USER appuser

CMD [ "./spotdb" ]

EXPOSE 8080 8081 6033
