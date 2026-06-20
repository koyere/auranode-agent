# AuraNode Agent — imagen mínima
# Build: docker build -t auranode-agent .
# Run:   docker run -d --name auranode-agent \
#          -e AURANODE_TOKEN=ant_xxx \
#          -v auranode-data:/var/lib/auranode \
#          auranode-agent

FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
ARG COMMIT=none
ARG DATE=unknown
RUN CGO_ENABLED=0 go build \
      -ldflags "-s -w -X main.version=${VERSION} -X main.commit=${COMMIT} -X main.date=${DATE}" \
      -o /out/auranode-agent ./cmd/auranode-agent

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/auranode-agent /usr/local/bin/auranode-agent
ENV AURANODE_DB_PATH=/var/lib/auranode/buffer.db
VOLUME ["/var/lib/auranode"]
ENTRYPOINT ["/usr/local/bin/auranode-agent"]
