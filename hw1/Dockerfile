FROM golang:1.23-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/service .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/dashboardgen ./cmd/dashboardgen
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/loadgen ./cmd/loadgen

FROM alpine:3.20
RUN addgroup -S app && adduser -S -G app app
USER app
COPY --from=build /out/service /service
COPY --from=build /out/dashboardgen /dashboardgen
COPY --from=build /out/loadgen /loadgen
EXPOSE 8080
ENTRYPOINT ["/service"]
