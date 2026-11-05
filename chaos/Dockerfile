ARG GO_VERSION=1.27

FROM golang:${GO_VERSION} AS build
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 go build -trimpath -o /chaos .

FROM gcr.io/distroless/static-debian12
COPY --from=build /chaos /chaos
ENTRYPOINT ["/chaos"]
