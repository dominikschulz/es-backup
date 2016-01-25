FROM golang:1.5.1
MAINTAINER Dominik Schulz <dominik.schulz@gauner.org>

ENV GOPATH /go/src/github.com/dominikschulz/es-backup/Godeps/_workspace/:/go

ADD .   /go/src/github.com/dominikschulz/es-backup
WORKDIR /go/src/github.com/dominikschulz/es-backup

RUN go install

CMD [ "/go/bin/es-backup" ]
