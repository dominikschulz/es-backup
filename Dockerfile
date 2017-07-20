FROM golang:1.8-alpine3.6 as builder

ADD . /go/src/github.com/dominikschulz/es-backup
WORKDIR /go/src/github.com/dominikschulz/es-backup

RUN go install

FROM alpine:3.6

COPY --from=builder /go/bin/es-backup /usr/local/bin/es-backup
CMD [ "/usr/local/bin/es-backup" ]
EXPOSE 8080
