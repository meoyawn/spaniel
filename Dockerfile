FROM scratch

COPY dist/spaniel /spaniel
ENTRYPOINT ["/spaniel"]
