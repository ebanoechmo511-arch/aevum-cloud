# Build stage
FROM golang:1.22-alpine AS builder

WORKDIR /app
COPY . .
RUN rm -f go.sum && go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o aevum-cloud .

# Runtime stage: ultra-compact alpine
FROM alpine:3.19
RUN apk --no-cache add ca-certificates

WORKDIR /root/
COPY --from=builder /app/aevum-cloud .

ENV PORT=8080
EXPOSE 8080

CMD ["./aevum-cloud"]
