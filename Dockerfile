FROM node:26-alpine@sha256:e88a35be04478413b7c71c455cd9865de9b9360e1f43456be5951032d7ac1a66 AS ui-build
WORKDIR /app
COPY ui/package*.json ./ui/
RUN cd ui && npm ci
COPY ui/ ./ui/
# Create output directory and build - vite outputs directly to pkg/github/ui_dist/
RUN mkdir -p ./pkg/github/ui_dist && \
    cd ui && npm run build

FROM golang:1.27.3-alpine@sha256:f2c872bcee949b7be9c490efd02b48a8a2fbf947cdf533f1c58525ec9d8170fd AS build
ARG VERSION="dev"
ARG ACMM_SOURCE_COMMIT="unknown"
ARG BUILD_DATE="unknown"

# Set the working directory
WORKDIR /build

# Install git for Go module fallback paths. Build provenance itself is supplied
# explicitly through ACMM_SOURCE_COMMIT and does not depend on the build
# context containing .git metadata.
RUN --mount=type=cache,target=/var/cache/apk \
    apk add git

# Copy source code (including ui_dist placeholder)
COPY . .

# Copy built UI assets over the placeholder
COPY --from=ui-build /app/pkg/github/ui_dist/* ./pkg/github/ui_dist/

# Build the server. -buildvcs=false prevents Windows/WSL checkout metadata from
# changing the embedded source identity. The authoritative ACMM source commit
# is supplied explicitly by the caller after the governor changes are committed.
# OAuth credentials are injected via build secrets so they are not baked into
# image history; the values are public in practice but kept out of layers.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=secret,id=oauth_client_id \
    --mount=type=secret,id=oauth_client_secret \
    export OAUTH_CLIENT_ID="$(cat /run/secrets/oauth_client_id 2>/dev/null || echo '')" && \
    export OAUTH_CLIENT_SECRET="$(cat /run/secrets/oauth_client_secret 2>/dev/null || echo '')" && \
    CGO_ENABLED=0 go build -buildvcs=false \
    -ldflags="-s -w -X main.version=${VERSION} -X main.commit=${ACMM_SOURCE_COMMIT} -X main.date=${BUILD_DATE} -X github.com/github/github-mcp-server/v2/internal/buildinfo.OAuthClientID=${OAUTH_CLIENT_ID} -X github.com/github/github-mcp-server/v2/internal/buildinfo.OAuthClientSecret=${OAUTH_CLIENT_SECRET}" \
    -o /bin/github-mcp-server ./cmd/github-mcp-server

# Make a stage to run the app
FROM gcr.io/distroless/base-debian12@sha256:fabbf1c0c357a3d42550111351daed089b20a2c954df13ee2fcff60602515e84
ARG ACMM_UPSTREAM_VERSION="v2.1.0"
ARG ACMM_UPSTREAM_COMMIT="4bcdcdf8d7beee98c9448e73e257308280924cc4"
ARG ACMM_SOURCE_COMMIT="unknown"
ARG ACMM_GOVERNOR_VERSION="v1"

# Add required MCP server annotation and ACMM provenance labels.
LABEL io.modelcontextprotocol.server.name="io.github.github/github-mcp-server" \
      io.acmm.github-mcp.upstream-version="${ACMM_UPSTREAM_VERSION}" \
      io.acmm.github-mcp.upstream-commit="${ACMM_UPSTREAM_COMMIT}" \
      io.acmm.github-mcp.source-commit="${ACMM_SOURCE_COMMIT}" \
      io.acmm.github-mcp.governor-version="${ACMM_GOVERNOR_VERSION}"

# Set the working directory
WORKDIR /server
# Copy the binary from the build stage
COPY --from=build /bin/github-mcp-server .
# Expose the default port
EXPOSE 8082
# Set the entrypoint to the server binary
ENTRYPOINT ["/server/github-mcp-server"]
# Default arguments for ENTRYPOINT
CMD ["stdio"]
