# Security model

This document describes every security mechanism in tsink — TLS, bearer-token authentication, role-based access control (RBAC), OIDC/JWT validation, cluster mTLS, multi-tenant isolation, secret rotation, and audit logging.

Security is enforced entirely at the server layer. The embedded library (`tsink` crate) has no authentication of its own; access control there is the responsibility of the embedding application.

---

## Contents

1. [Transport security (TLS)](#1-transport-security-tls)
2. [Authentication — bearer tokens](#2-authentication--bearer-tokens)
   - [Request scopes](#21-request-scopes)
   - [Public and admin tokens](#22-public-and-admin-tokens)
3. [Role-based access control (RBAC)](#3-role-based-access-control-rbac)
   - [Config file structure](#31-config-file-structure)
   - [Roles and grants](#32-roles-and-grants)
   - [Principals](#33-principals)
   - [Service accounts](#34-service-accounts)
   - [Authorization flow](#35-authorization-flow)
4. [OIDC / JWT authentication](#4-oidc--jwt-authentication)
   - [Provider configuration](#41-provider-configuration)
   - [Supported algorithms](#42-supported-algorithms)
   - [Claim validation](#43-claim-validation)
   - [Claim-to-role mappings](#44-claim-to-role-mappings)
5. [Cluster security](#5-cluster-security)
   - [Internal mTLS](#51-internal-mtls)
   - [Internal bearer token](#52-internal-bearer-token)
   - [Verified node-ID propagation](#53-verified-node-id-propagation)
6. [Multi-tenant isolation](#6-multi-tenant-isolation)
   - [Tenant identification](#61-tenant-identification)
   - [Per-tenant auth tokens](#62-per-tenant-auth-tokens)
   - [Per-tenant request and admission policies](#63-per-tenant-request-and-admission-policies)
7. [Secret rotation](#7-secret-rotation)
   - [Rotation targets](#71-rotation-targets)
   - [Reload vs. rotate](#72-reload-vs-rotate)
   - [Material backends](#73-material-backends)
   - [Overlap window](#74-overlap-window)
8. [Audit logging](#8-audit-logging)
   - [RBAC audit ring](#81-rbac-audit-ring)
   - [Security audit ring](#82-security-audit-ring)
   - [Cluster audit log](#83-cluster-audit-log)
9. [Spoofing-resistant header model](#9-spoofing-resistant-header-model)
10. [CLI security flags reference](#10-cli-security-flags-reference)

---

## 1. Transport security (TLS)

tsink uses [rustls](https://github.com/rustls/rustls) for all TLS — no OpenSSL dependency.

To enable TLS on the public listener, supply a PEM certificate and key:

```bash
tsink-server \
  --tls-cert /etc/tsink/server.crt \
  --tls-key  /etc/tsink/server.key \
  ...
```

Both files are read at startup. Certificates can be hot-reloaded or rotated at runtime without restarting the server (see [Secret rotation](#7-secret-rotation)).

When neither `--tls-cert`/`--tls-key` nor the cluster mTLS flags are given, the server listens in plain HTTP mode. Plain HTTP is acceptable for development or when TLS is terminated upstream (e.g. by a load balancer).

---

## 2. Authentication — bearer tokens

### 2.1 Request scopes

Every incoming HTTP request is classified into one of four scopes before authentication is checked:

| Scope | Path prefix | Access rule |
|---|---|---|
| `Probe` | `/healthz`, `/ready` | Always allowed — no token required. |
| `Internal` | `/internal/v1/*` | Cluster-peer traffic; authenticated at the transport layer by mTLS or the internal bearer token. Not accessible from the public network. |
| `Admin` | `/api/v1/admin/*` | Requires the admin token (or the public token when no separate admin token is configured). Enabled only with `--enable-admin-api`. |
| `Public` | everything else | Requires the public token when one is configured. |

### 2.2 Public and admin tokens

Two independent bearer tokens can be configured:

| Role | Flag | Description |
|---|---|---|
| Public | `--auth-token` / `--auth-token-file` | Required for all data-plane endpoints (`/api/v1/query`, `/write`, `/v1/metrics`, …). |
| Admin | `--admin-auth-token` / `--admin-auth-token-file` | Required for admin endpoints (`/api/v1/admin/*`). Falls back to the public token if no dedicated admin token is set. |

The token is carried in the `Authorization` header:

```
Authorization: Bearer <token>
```

If the public token is configured and the request provides no token or a wrong token, the server returns `401 Unauthorized`. Presenting a public token to an admin endpoint when a separate admin token is configured returns `403 Forbidden` (`auth_scope_denied`).

When **no** public token is configured, the public endpoints are unauthenticated. Securing the listener with TLS is still strongly recommended even without a token.

---

## 3. Role-based access control (RBAC)

RBAC is an optional layer on top of bearer-token authentication. Enable it by pointing `--rbac-config` at a JSON file. When an RBAC config is present every request is additionally authorised against the role graph; when it is absent the simple token check described in §2 applies.

### 3.1 Config file structure

```jsonc
{
  "roles": {
    "<role-name>": {
      "grants": [
        { "action": "Read|Write", "resource": { "kind": "Tenant|Admin|System", "name": "<name-or-wildcard>" } }
      ]
    }
  },
  "principals": [
    {
      "id": "<principal-id>",
      "token": "<bearer-token>",
      "bindings": [
        { "role": "<role-name>", "scopes": [ { "kind": "Tenant", "name": "acme" } ] }
      ]
    }
  ],
  "service_accounts": [
    {
      "id": "<sa-id>",
      "token": "<bearer-token>",
      "bindings": [ { "role": "<role-name>" } ]
    }
  ],
  "oidc_providers": [ ... ]
}
```

The config is hot-reloaded via `POST /api/v1/admin/rbac/reload`.

### 3.2 Roles and grants

A role is a named set of grants. Each grant pairs an **action** with a **resource**:

| Field | Values |
|---|---|
| `action` | `Read` or `Write` |
| `resource.kind` | `Tenant` — scoped to data-plane operations for a named tenant. `Admin` — administrative operations. `System` — system-level operations. |
| `resource.name` | Exact name, prefix wildcard (`metrics*`), or `*` for all resources of that kind. |

Example — a role that can write to any tenant and read from the `ops` tenant only:

```json
{
  "data-writer": {
    "grants": [
      { "action": "Write", "resource": { "kind": "Tenant", "name": "*" } },
      { "action": "Read",  "resource": { "kind": "Tenant", "name": "ops" } }
    ]
  }
}
```

### 3.3 Principals

A principal is a named identity backed by a static bearer token. Principals are defined in the RBAC config file and are typically used for long-lived service identities.

```json
{
  "id": "ingestor-1",
  "token": "tok_ingestor_abc123",
  "bindings": [
    { "role": "data-writer", "scopes": [ { "kind": "Tenant", "name": "acme" } ] }
  ]
}
```

A binding links a principal to a role, optionally restricted to a subset of resources via `scopes`. Omitting `scopes` means the binding applies to all resources the role grants access to.

### 3.4 Service accounts

Service accounts are managed programmatically through the admin API and support token rotation without reloading the config file:

```
POST   /api/v1/admin/rbac/service-accounts          Create
GET    /api/v1/admin/rbac/service-accounts/{id}     Describe
POST   /api/v1/admin/rbac/service-accounts/{id}/rotate   Rotate token
POST   /api/v1/admin/rbac/service-accounts/{id}/disable  Disable
```

Service account tokens are prefixed `tsa_` and are generated from 32 cryptographically random bytes (base64url, no padding).

### 3.5 Authorization flow

When a request arrives and RBAC is active:

1. The bearer token is extracted from the `Authorization` header.
2. An O(1) lookup in the token index resolves the token to a principal or service account identity.
3. If no match is found and the token contains two `.` separators it is treated as a JWT → OIDC path (see §4).
4. Otherwise the server returns `401 auth_token_invalid`.
5. If the resolved identity is disabled the server returns `403 auth_principal_disabled`.
6. The binding graph is walked; if any role grant covers the requested action and resource the request is admitted and the identity is stamped onto the request context.
7. If no grant matches the server returns `403 auth_scope_denied`.

All decisions — both allow and deny — are written to the RBAC audit ring (see §8.1).

---

## 4. OIDC / JWT authentication

OIDC providers are declared in the RBAC config file's `oidc_providers` array. This allows identity tokens issued by external providers (Keycloak, Okta, Auth0, Google, etc.) to be used as bearer tokens.

### 4.1 Provider configuration

```jsonc
{
  "oidc_providers": [
    {
      "name": "my-idp",
      "issuer": "https://idp.example.com",
      "audiences": ["tsink"],
      "username_claim": "email",
      "jwks_url": "https://idp.example.com/.well-known/jwks.json",
      "claim_mappings": [
        {
          "claim": "groups",
          "value": "tsink-admins",
          "bindings": [ { "role": "admin" } ]
        }
      ]
    }
  ]
}
```

| Field | Required | Description |
|---|---|---|
| `name` | Yes | Unique provider identifier used in audit entries and the derived principal ID. |
| `issuer` | Yes | Expected `iss` claim value. |
| `audiences` | No | If non-empty, at least one value must match the JWT `aud` claim. |
| `username_claim` | No | Claim to use as the display name; defaults to `sub`. |
| `jwks_url` | No* | URL of the provider's JWKS endpoint. Fetched once at config load (5 s timeout). |
| `jwks` | No* | Inline array of JWK objects. Use instead of `jwks_url` for air-gapped deployments. |
| `claim_mappings` | No | Rules that map JWT claim values to RBAC role bindings. |

\* At least one of `jwks_url` or `jwks` must be provided.

### 4.2 Supported algorithms

JWT signature verification is implemented directly with the `ring` cryptography library. No third-party JWT library is used.

| Algorithm | Key type |
|---|---|
| `RS256` | RSA PKCS#1 (2048–8192 bit) |
| `ES256` | ECDSA P-256 |
| `HS256` | HMAC-SHA256 (symmetric) |

The algorithm is taken from the JWK's `alg` field; no algorithm downgrade is possible because only the algorithm that matches the configured key's type is attempted.

### 4.3 Claim validation

| Claim | Validation |
|---|---|
| `exp` | Required; token must not be expired. A 60-second clock-skew tolerance is applied. |
| `nbf` | Optional; if present, token must have reached its valid-from time (60-second skew). |
| `iat` | Optional; checked as a sanity bound (60-second skew). |
| `iss` | Must match the `issuer` field of the provider definition. |
| `aud` | Must contain at least one configured audience value (if audiences are configured). |

An expired token returns `401 auth_oidc_token_expired`. A token with an unrecognised issuer or algorithm returns `401 auth_token_invalid`.

### 4.4 Claim-to-role mappings

A `claim_mapping` entry matches a single JWT claim against a value pattern and, when it matches, injects RBAC role bindings for the request:

```jsonc
{
  "claim": "groups",      // claim name (scalar string or JSON array in the token)
  "value": "ops-*",       // exact, prefix wildcard, or "*"
  "bindings": [ { "role": "observer" } ]
}
```

If the claim value is an array all elements are tested. The derived principal ID is `oidc:<provider-name>:<sub>`.

---

## 5. Cluster security

### 5.1 Internal mTLS

When clustering is enabled, peer-to-peer RPC traffic on `/internal/v1/*` can be protected with mutual TLS using a dedicated internal CA:

```bash
tsink-server \
  --cluster-internal-mtls-enabled true \
  --cluster-internal-mtls-ca-cert /etc/tsink/cluster-ca.crt \
  --cluster-internal-mtls-cert    /etc/tsink/node.crt \
  --cluster-internal-mtls-key     /etc/tsink/node.key \
  ...
```

| Flag | Description |
|---|---|
| `--cluster-internal-mtls-ca-cert` | PEM CA bundle used to verify client certs presented by peer nodes. |
| `--cluster-internal-mtls-cert` | PEM certificate presented by this node when connecting to peers as a client. |
| `--cluster-internal-mtls-key` | Corresponding PEM private key. |

The same CA bundle is also used as the client-CA on the public listener, so inbound `/internal/v1/*` requests are rejected at the TLS handshake unless they present a certificate signed by the cluster CA. The public listener and the cluster listener share one acceptor; the client-cert requirement applies only when `cluster-internal-mtls` is enabled.

### 5.2 Internal bearer token

As an alternative to mTLS, cluster peers can authenticate to each other with a shared bearer token:

```bash
--cluster-internal-auth-token-file /run/secrets/cluster-token
```

The internal token is checked only on `/internal/v1/*` paths. It is never accepted on public endpoints.

> mTLS and the internal bearer token are independent — you can use either, both, or neither (the last option is appropriate when the internal network is trusted, e.g. a private Kubernetes namespace).

### 5.3 Verified node-ID propagation

When a cluster peer connects over mTLS the server extracts the node ID from the client certificate (Common Name or first DNS SAN label) and stamps it into the internal header `x-tsink-internal-verified-node-id`. This header is **always stripped from inbound requests** before any handler sees it, so it can never be spoofed by an external client.

Similarly, all RBAC-verified identity headers (`x-tsink-rbac-verified`, `x-tsink-auth-principal-id`, `x-tsink-auth-role`, etc.) are stripped on ingress. See §9 for the full list.

---

## 6. Multi-tenant isolation

### 6.1 Tenant identification

Each request carries a tenant identifier in the `x-tsink-tenant` header. When the header is absent the request is attributed to the `default` tenant. The tenant ID is injected as the internal label `__tsink_tenant__` on every stored series, providing hard data-plane isolation between tenants that share a single server.

### 6.2 Per-tenant auth tokens

The tenant config file (supplied via `--tenant-config`) supports per-tenant bearer tokens with granular scope:

```jsonc
{
  "tenants": [
    {
      "id": "acme",
      "auth": {
        "tokens": [
          { "token": "acme-write-token", "scopes": ["Write"] },
          { "token": "acme-read-token",  "scopes": ["Read"] }
        ]
      }
    }
  ]
}
```

A request carrying a valid per-tenant `Write` token for tenant `acme` is allowed to ingest data into that tenant only; it cannot read data or access other tenants. Per-tenant token auth is evaluated before the global `SecurityManager` token check.

### 6.3 Per-tenant request and admission policies

Each tenant can have independent request size limits and concurrency budgets:

```jsonc
{
  "id": "acme",
  "request_policy": {
    "max_write_rows_per_request": 10000,
    "max_read_queries_per_request": 5,
    "max_query_length_bytes": 4096,
    "max_range_points_per_query": 1000000,
    "admission": {
      "ingest":    { "max_inflight_requests": 50, "max_inflight_units": 100000 },
      "query":     { "max_inflight_requests": 20 },
      "metadata":  { "max_inflight_requests": 10 },
      "retention": { "max_inflight_requests": 5  }
    }
  }
}
```

Admission budgets are enforced with tokio semaphores. When the budget is exhausted new requests for that tenant are rejected with `429 Too Many Requests` rather than queuing indefinitely, preventing one tenant's load from starving others.

---

## 7. Secret rotation

All security material — bearer tokens and TLS certificates — can be rotated at runtime without restarting the server. The rotation API is available at `POST /api/v1/admin/security/rotate` (requires the admin token and `--enable-admin-api`).

### 7.1 Rotation targets

| Target | Covers |
|---|---|
| `PublicAuthToken` | The public bearer token (`--auth-token` / `--auth-token-file`). |
| `AdminAuthToken` | The admin bearer token. |
| `ClusterInternalAuthToken` | The shared internal cluster auth token. |
| `ListenerTls` | The TLS certificate and key for the public listener. |
| `ClusterInternalMtls` | The client certificate and key used for cluster peer connections. |

### 7.2 Reload vs. rotate

| Mode | Behaviour |
|---|---|
| `Reload` | Re-reads the material from its original source (file or exec command). The new material replaces the current one; the previous value is retained as a fallback during the overlap window. |
| `Rotate` | Invokes the `rotateCommand` from the exec manifest (or generates a new random token) to produce new material, writes it to disk atomically, then reloads. |

### 7.3 Material backends

A secret path is interpreted as:

- **Plain file** — the file content is read as the secret value.
- **Exec manifest** — if the file begins with `{`, it is parsed as JSON:

```jsonc
{
  "kind": "exec",
  "command": ["vault", "read", "-field=token", "secret/tsink/public"],
  "rotateCommand": ["vault", "write", "secret/tsink/public"]
}
```

`command` is executed to load or reload the current value. `rotateCommand` is executed during a `Rotate` operation; if empty the target is not rotatable via the exec path. This allows integration with any secrets management system (HashiCorp Vault, AWS Secrets Manager via CLI, etc.).

Atomic file writes during rotation use a unique hidden temporary file followed by `rename`, ensuring no reader ever sees a partial write. On Unix-like systems, rotated token files are written with `0600` permissions.

### 7.4 Overlap window

When material is replaced (either by reload or rotation) the previous value is kept alive for a configurable overlap period (default **300 seconds**). During this window the server accepts both the old and the new credential, enabling zero-downtime rotation when clients are updated gradually. After the overlap window expires the old credential is discarded.

The overlap duration can be overridden per rotation call via the `overlap_seconds` request field.

---

## 8. Audit logging

tsink maintains three independent audit systems.

### 8.1 RBAC audit ring

An in-memory ring buffer of the last **256** authorization decisions is maintained by the RBAC engine. Every `authorize()` call — whether it succeeds or is denied — produces an entry. Administrative operations (service account create/update/rotate/disable, config reload) are also recorded.

Query the ring:

```
GET /api/v1/admin/rbac/audit?limit=100
```

Each entry contains:

| Field | Description |
|---|---|
| `sequence` | Monotonically increasing event counter. |
| `timestamp_unix_ms` | Event time. |
| `event` | Event type (e.g. `Authorize`, `ServiceAccountCreated`, `ConfigReloaded`). |
| `outcome` | `Allow` or `Deny`. |
| `principal_id` | Resolved identity (`oidc:<provider>:<sub>` for OIDC tokens). |
| `role` | Role that matched (allow path). |
| `action` | `Read` or `Write`. |
| `resource` | Resource kind and name. |
| `code` | Error code for denied requests (e.g. `auth_scope_denied`). |
| `auth_method` | `Token`, `ServiceAccount`, or `Oidc`. |
| `provider` | OIDC provider name (OIDC path only). |
| `subject` | JWT `sub` claim (OIDC path only). |
| `detail` | Free-text supplementary information. |

### 8.2 Security audit ring

A separate in-memory ring of the last **128** entries covers all secret lifecycle events:

| Field | Description |
|---|---|
| `sequence` | Monotonically increasing event counter. |
| `timestamp_unix_ms` | Event time. |
| `target` | Which secret was affected (`PublicAuthToken`, `ListenerTls`, etc.). |
| `operation` | `Reload` or `Rotate`. |
| `outcome` | `Success` or `Failure`. |
| `actor` | Principal that triggered the operation. |
| `detail` | Error message on failure. |

Query via `GET /api/v1/admin/security/audit`.

### 8.3 Cluster audit log

A persistent JSONL file records cluster-level control-plane events. Every record is fsynced to disk on write.

Default limits (all tunable via environment variables):

| Variable | Default | Description |
|---|---|---|
| `TSINK_CLUSTER_AUDIT_RETENTION_SECS` | 2 592 000 (30 days) | Records older than this are pruned on each append. |
| `TSINK_CLUSTER_AUDIT_MAX_LOG_BYTES` | 134 217 728 (128 MiB) | When exceeded the log is compacted (rewritten without pruned records). |
| `TSINK_CLUSTER_AUDIT_MAX_QUERY_LIMIT` | 1 000 | Maximum records returned per query. |

Each `ClusterAuditRecord` contains:

| Field | Description |
|---|---|
| `id` | Unique record ID. |
| `timestamp_unix_ms` | Event time. |
| `operation` | Operation name (e.g. `RebalanceStarted`, `SnapshotCompleted`). |
| `actor.id` | Principal that initiated the operation. |
| `actor.auth_scope` | `Public` or `Admin`. |
| `target` | Arbitrary JSON payload describing the affected resource. |
| `outcome.status` | `Success` or `Failure`. |
| `outcome.http_status` | HTTP status code. |
| `outcome.error_type` | Error classifier on failure. |

Query the log:

```
GET /api/v1/admin/cluster/audit
    ?operation=RebalanceStarted
    &actor_id=admin-principal
    &status=Success
    &since_unix_ms=1700000000000
    &until_unix_ms=1800000000000
    &limit=500
```

---

## 9. Spoofing-resistant header model

Several internal headers carry security-sensitive metadata set by the server during request processing. To prevent clients from injecting forged values, all of these headers are **stripped from every inbound request** before any handler executes.

| Header | Set by | Carries |
|---|---|---|
| `x-tsink-rbac-verified` | RBAC engine | Presence flag when RBAC authorisation succeeded. |
| `x-tsink-auth-principal-id` | RBAC engine | Resolved principal or service account ID. |
| `x-tsink-auth-role` | RBAC engine | Matched role name. |
| `x-tsink-auth-method` | RBAC engine | `Token`, `ServiceAccount`, or `Oidc`. |
| `x-tsink-auth-provider` | RBAC engine | OIDC provider name (OIDC path only). |
| `x-tsink-auth-subject` | RBAC engine | JWT `sub` claim (OIDC path only). |
| `x-tsink-internal-verified-node-id` | Transport layer | Peer node ID extracted from mTLS client certificate CN / SAN. |
| `x-tsink-public-auth-required` | Security layer | Whether a public auth token is configured. |
| `x-tsink-public-auth-verified` | Security layer | Whether the request passed public token check. |

None of these headers should be forwarded by reverse proxies or load balancers. If tsink sits behind a proxy, ensure the proxy does not pass client-supplied headers with these names.

---

## 10. CLI security flags reference

| Flag | Type | Description |
|---|---|---|
| `--tls-cert PATH` | Path | PEM certificate file for the public listener. Enables TLS when combined with `--tls-key`. |
| `--tls-key PATH` | Path | PEM private key for the public listener. |
| `--auth-token TOKEN` | String | Inline public bearer token. Mutually exclusive with `--auth-token-file`. |
| `--auth-token-file PATH` | Path | Public bearer token loaded from a plain file or exec manifest. |
| `--admin-auth-token TOKEN` | String | Inline admin bearer token. When set, admin endpoints require this token instead of the public token. |
| `--admin-auth-token-file PATH` | Path | Admin bearer token from a plain file or exec manifest. |
| `--rbac-config PATH` | Path | Path to the RBAC JSON configuration file (roles, principals, service accounts, OIDC providers). |
| `--tenant-config PATH` | Path | Path to the multi-tenant JSON configuration file. |
| `--enable-admin-api` | Flag | Unlock the `/api/v1/admin/*` endpoint group. Off by default. |
| `--admin-path-prefix PATH` | Path | Restrict admin file-system operations (e.g. cert rotation) to files under this prefix. |
| `--cluster-internal-auth-token TOKEN` | String | Shared bearer token for cluster peer RPC. |
| `--cluster-internal-auth-token-file PATH` | Path | Same, from a plain file or exec manifest. |
| `--cluster-internal-mtls-enabled BOOL` | Bool | Enable mutual TLS for `/internal/v1/*` cluster traffic. |
| `--cluster-internal-mtls-ca-cert PATH` | Path | PEM CA bundle used to verify client certificates from peer nodes. |
| `--cluster-internal-mtls-cert PATH` | Path | PEM client certificate presented by this node on outbound cluster connections. |
| `--cluster-internal-mtls-key PATH` | Path | PEM private key for the cluster client certificate. |
