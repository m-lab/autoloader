FROM golang:1.20

RUN apt-get update && apt-get install -y --no-install-recommends clang
ENV CGO_ENABLED=1
ENV CXX=clang++
RUN go install github.com/goccy/bigquery-emulator/cmd/bigquery-emulator@latest