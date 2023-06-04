# Build Stage
FROM golang:1.17.2-alpine3.14 as build

WORKDIR /go/src/app
COPY . .

RUN apk add --no-cache tzdata
RUN go build -v -o main-app

# Run Stage
FROM golang:1.17.2-alpine3.14

COPY --from=build go/src/app app/

RUN apk add --no-cache tzdata protobuf mysql-client git \
  && go get github.com/golang/protobuf/protoc-gen-go \
  && cp /go/bin/protoc-gen-go /usr/bin/

CMD ["./app/main-app"]