FROM registry-vpc.cn-beijing.aliyuncs.com/shimobase/golang:1.15 as base

WORKDIR /src

COPY go.mod .
COPY go.sum .

ENV GOPROXY https://goproxy.cn

RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /usr/local/bin/job-tree ./bin/tree/RunTreeJob.go
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /usr/local/bin/job-file ./bin/files/RunFileJob.go
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /usr/local/bin/job-script ./bin/script/RunScriptJob.go

COPY ./bin/RunJob.sh /usr/local/bin
COPY ./saas.json /usr/local/bin/saas.json
COPY ./dev.json /usr/local/bin/dev.json

FROM registry-vpc.cn-beijing.aliyuncs.com/shimobase/alpine:3.7 as final

WORKDIR /usr/local/bin
COPY --from=base /usr/local/bin/job-tree .
COPY --from=base /usr/local/bin/job-file .
COPY --from=base /usr/local/bin/job-script .
COPY --from=base /usr/local/bin/RunJob.sh .
COPY --from=base /usr/local/bin/saas.json .
COPY --from=base /usr/local/bin/dev.json .
COPY --from=base /src/init.sh .
COPY --from=base /src/script/ ./script/

RUN apk add --no-cache tzdata
RUN /bin/cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && echo 'Asia/Shanghai' >/etc/timezone
ENV ENV production

CMD ["/bin/sh","init.sh"]
