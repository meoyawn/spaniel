FROM scratch

COPY dist/spaniel-server /spaniel-server
ENTRYPOINT ["/spaniel-server"]
