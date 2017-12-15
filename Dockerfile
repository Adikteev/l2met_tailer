FROM golang:1.9.2

WORKDIR /go/src/app

RUN git clone "https://github.com/edenhill/librdkafka.git" && \
  cd librdkafka && \
  git checkout 261371d && \
  ./configure --prefix /usr && \
  make && \
  make install

COPY . .

RUN go-wrapper download   # "go get -d -v ./..."
RUN go-wrapper install    # "go install -v ./..."

CMD ["go-wrapper", "run"] # ["app"]

