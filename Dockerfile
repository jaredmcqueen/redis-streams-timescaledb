# build stage
FROM golang:alpine AS builder
WORKDIR /app
COPY . .
RUN go build -o main main.go

# run stage
FROM alpine:latest as deploy
WORKDIR /app
COPY --from=builder /app/main .
COPY app.env .

ENTRYPOINT [ "/app/main" ]
