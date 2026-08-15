FROM golang:1.26.3-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -buildvcs=false -ldflags="-s -w -buildid=" -o /spaniel ./cmd/spaniel

FROM scratch

COPY --from=build /spaniel /spaniel
EXPOSE 4318
ENTRYPOINT ["/spaniel"]
CMD ["-addr", ":4318"]
