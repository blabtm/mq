FROM golang:1.24 AS build

WORKDIR /app

COPY go.mod .
COPY go.sum .
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /mq ./cmd/docker

FROM debian:buster-slim

WORKDIR /
COPY --from=build /mq /mq

EXPOSE 1883
EXPOSE 20041
ENTRYPOINT [ "/mq" ]
