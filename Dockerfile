# Start by building the application.
FROM golang:1.20-buster as build

WORKDIR /go/src/app
ADD . /go/src/app

RUN go mod download

RUN go build -o /go/bin/rabbitmq-tester

# Now copy it into our base image.
FROM gcr.io/distroless/base-debian10
COPY --from=build /go/bin/rabbitmq-tester /
ENTRYPOINT ["/rabbitmq-tester"]
