FROM golang:1.26

WORKDIR /usr/src/notifier-service

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -v -o /usr/local/bin/server ./cmd/server

# запустится, т.к. в PATH лежит путь /usr/local/bin/server
CMD ["server"]