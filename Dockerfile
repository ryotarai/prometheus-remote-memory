FROM golang:1.11 AS builder
WORKDIR /go/src/app
COPY . .
RUN go build -o /usr/bin/prometheus-remote-memory .

###############################################

FROM ubuntu:16.04
COPY --from=builder /usr/bin/prometheus-remote-memory /usr/bin/prometheus-remote-memory

ENTRYPOINT ["/usr/bin/prometheus-remote-memory"]
