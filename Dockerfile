FROM golang:1.27.4@sha256:205c7636d74e0ffbe3d007162df7d74f859f5a8c8322dfb9f907977686c126cf AS build
WORKDIR /src
ENV GOPROXY=off GOFLAGS=-mod=readonly

COPY go.mod go.sum ./
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
