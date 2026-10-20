# ==============================================================================
# Dockerfile para MinIO (Compilado desde el código fuente oficial de GitHub)
# Soluciona la eliminación de imágenes oficiales de MinIO en Docker Hub.
# ==============================================================================

# Etapa 1: Compilar MinIO desde el repositorio oficial de GitHub
FROM golang:1.24-alpine AS builder

RUN apk add --no-cache git

WORKDIR /go/src/github.com/minio/minio
RUN git clone --depth 1 https://github.com/minio/minio.git .

ENV CGO_ENABLED=0
ENV GO111MODULE=on
RUN go build -trimpath --ldflags "-s -w" -o /go/bin/minio main.go

# Etapa 2: Imagen final limpia y ligera
FROM alpine:3.20

RUN apk --no-cache add ca-certificates

COPY --from=builder /go/bin/minio /usr/bin/minio

EXPOSE 9000 9001
VOLUME ["/data"]

ENTRYPOINT ["/usr/bin/minio"]
CMD ["server", "/data", "--console-address", ":9001"]
