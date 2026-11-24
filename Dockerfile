# syntax=docker/dockerfile:1

FROM golang:1.27.4 AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY cmd ./cmd
COPY internal ./internal

ARG TARGETOS=linux
ARG TARGETARCH=amd64
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /out/bunpo-server ./cmd/bunpo-server \
    && install -d -m 0700 -o 65532 -g 65532 /out/data

FROM scratch
COPY --from=build /out/bunpo-server /bunpo-server
COPY --from=build --chown=65532:65532 /out/data /data

USER 65532:65532
VOLUME ["/data"]
EXPOSE 8080
ENTRYPOINT ["/bunpo-server"]
CMD ["--addr", "0.0.0.0:8080", "--data", "/data"]
