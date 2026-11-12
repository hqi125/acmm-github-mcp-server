# ACMM GitHub API Governor

This fork is based on GitHub MCP Server v2.1.0 at upstream commit
`4bcdcdf8d7beee98c9448e73e257308280924cc4`.

The ACMM governor is an HTTP `RoundTripper` wrapper shared process-wide across
GitHub API traffic. It is intentionally below the existing authentication,
user-agent, GraphQL-feature and ETag transports so the MCP tool surface and
request semantics stay upstream-compatible while actual provider traffic is
paced centrally.

## Behavior

- All governed upstream requests are serialized.
- Request starts are spaced by at least 500 ms.
- POST, PUT, PATCH and DELETE starts are spaced by at least 2 s. GraphQL uses
  POST and therefore receives the mutative spacing conservatively.
- `Retry-After` is honored with a 3 s safety margin.
- Primary exhaustion (`X-RateLimit-Remaining: 0`) honors
  `X-RateLimit-Reset` with a 3 s safety margin.
- Secondary limits without provider timing use persistent backoff of
  1, 2, 4, 8 and 16 minutes. After the configured sequence is exhausted, the
  provider response is returned rather than continuing to retry.
- Mutative requests are never automatically replayed. A rate-limit response
  records the cooldown and is returned to the caller.
- Safe read-like methods (GET, HEAD and OPTIONS) may retry after an explicit
  rate-limit response when their body is replayable.
- Context cancellation aborts governor waiting and prevents a cancelled action
  from dispatching or being replayed.
- The configured ACMM unadmitted sentinel token is rejected locally with 401
  and never sent to GitHub.
- Persistent state contains only non-secret traffic telemetry and timing data.

## Environment

| Variable | Default | Notes |
| --- | --- | --- |
| `ACMM_GITHUB_MCP_GOVERNOR_ENABLED` | `true` | Set false only for engineering comparison. |
| `ACMM_GITHUB_MCP_REQUEST_INTERVAL` | `500ms` | Values below the default are raised to the floor. |
| `ACMM_GITHUB_MCP_MUTATION_INTERVAL` | `2s` | Values below the default are raised to the floor. |
| `ACMM_GITHUB_MCP_RETRY_AFTER_GRACE` | `3s` | Safety margin for `Retry-After`. |
| `ACMM_GITHUB_MCP_PRIMARY_RESET_GRACE` | `3s` | Safety margin for primary reset time. |
| `ACMM_GITHUB_MCP_SECONDARY_BACKOFF` | `60s,120s,240s,480s,960s` | Five-step persistent sequence; weaker values are raised to the defaults. |
| `ACMM_GITHUB_MCP_GOVERNOR_STATE_FILE` | empty | JSON persistence path. Experiment 2 supplies a persistent volume path. |
| `ACMM_GITHUB_MCP_UNADMITTED_TOKEN` | empty | Sentinel rejected locally before provider traffic. |

## Governed paths

The v2.1.0 fork wires the shared governor into:

- HTTP-mode REST requests;
- HTTP-mode GraphQL requests;
- HTTP-mode OAuth-scope HEAD requests;
- stdio REST requests, beneath the ETag transport;
- stdio GraphQL requests;
- stdio raw-content requests;
- stdio OAuth-scope HEAD requests.

The raw HTTP-mode client is derived from the governed REST client and therefore
inherits the same transport.

## Build provenance

The Docker image records four ACMM labels:

- `io.acmm.github-mcp.upstream-version`
- `io.acmm.github-mcp.upstream-commit`
- `io.acmm.github-mcp.source-commit`
- `io.acmm.github-mcp.governor-version`

The build uses `-buildvcs=false` and receives `ACMM_SOURCE_COMMIT` explicitly.
This avoids treating Windows/WSL working-tree metadata as authoritative source
provenance.
