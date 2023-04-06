FROM golang:1.20 as build
ENV CGO_ENABLED=0
WORKDIR /go/src/github.com/m-lab/autoloader
COPY . .
RUN go get -v ./...
RUN go install -v  \
      -ldflags "-X github.com/m-lab/go/prometheusx.GitShortCommit=$(git log -1 --format=%h)$(git diff --quiet || echo dirty)" \
      ./cmd/autoloader

FROM alpine:3.17
COPY --from=build /go/bin/autoloader /
WORKDIR /
RUN /autoloader -h 2> /dev/null
ENTRYPOINT ["/autoloader"]