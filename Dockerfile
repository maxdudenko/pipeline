FROM golang:latest AS compiling_stage
RUN mkdir -p /go/src/pipeline
WORKDIR /go/src/pipeline
ADD main.go .
ADD go.mod .
ENV CGO_ENABLED=0 GOOS=linux
RUN go build -o /go/bin/pipeline main.go

FROM alpine:latest
LABEL version="1.0.0"
LABEL maintainer="Maksim Dudenko<maxdudenko91@gmail.com>"
WORKDIR /root/
COPY --from=compiling_stage /go/bin/pipeline .
ENTRYPOINT ["./pipeline"]