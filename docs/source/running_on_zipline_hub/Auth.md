---
title: "Authentication"
order: 5
---

# Authentication

Zipline supports optional authentication to secure access to the platform. When enabled, users authenticate through the frontend (via OAuth or SSO), and backend services validate requests using signed JWTs.

## Overview

Authentication flows through three components:

1. **Frontend** — identity provider with OAuth, SSO (OIDC/SAML), JWT issuance, and device authorization for the CLI
2. **CLI (`zipline auth`)** — authenticates via the [device authorization flow (RFC 8628)](https://datatracker.ietf.org/doc/html/rfc8628), sends JWTs to backend services
3. **Backend services (Orchestration, Eval)** — validate JWTs statelessly via JWKS

## Enabling Authentication

### Frontend

Set the following environment variables on the frontend service:

| Variable | Required | Description |
|---|---|---|
| `AUTH_ENABLED` | Yes | Set to `true` to enable authentication |
| `AUTH_URL` | Yes | Base URL of the frontend (e.g., `https://zipline.example.com`). Used for OAuth redirect URIs and as the JWT issuer / JWKS origin |
| `AUTH_EXTERNAL_URL` | No | Public URL when the frontend is behind a reverse proxy (CloudFront, API Gateway, ingress) and `AUTH_URL` is an internal origin. Used for user-facing URLs — the SAML post-login redirect and the CLI device-login verification URL — so they resolve to the host users actually reach. `AUTH_URL` (and the JWT issuer / JWKS) stay internal. Leave unset when `AUTH_URL` is already the public URL |
| `AUTH_SECRET` | Yes | Random secret (32+ characters) used for signing keys. Generate with `openssl rand -base64 32` |
| `AUTH_ALLOWED_HOSTS` | No | Comma-separated list of additional allowed hosts for incoming requests (e.g., internal service names like `orchestration-ui-service`). `AUTH_URL` and `host.docker.internal:*` are always allowed |

You must also configure at least one authentication provider (see below).

> **Behind a reverse proxy:** If users reach Zipline through an external domain (CloudFront, API Gateway, an ingress) that forwards to a different internal origin, set `AUTH_URL` to the internal origin — so the JWT issuer and JWKS endpoint stay reachable by the backend services — and `AUTH_EXTERNAL_URL` to the public domain. User-facing URLs (the SAML post-login redirect and the `zipline auth login` device URL) then use the public domain while tokens keep the internal issuer. If you don't need the issuer to stay internal, you can instead set `AUTH_URL` to the public URL and omit `AUTH_EXTERNAL_URL`.

### Backend Services (Orchestration & Eval)

| Variable | Required | Description |
|---|---|---|
| `AUTH_ENABLED` | Yes | Set to `true` to enable JWT validation |
| `AUTH_JWKS_URL` | Yes | URL of the JWKS endpoint (e.g., `https://zipline.example.com/api/auth/jwks`) |

When `AUTH_ENABLED=true` but the JWKS endpoint is unavailable (e.g., frontend is down), protected routes return `503 Authentication service unavailable` rather than allowing unauthenticated access.

## Authentication Providers

Configure one or more of the following providers. Each provider is enabled by setting its respective environment variables.

### Google OAuth

| Variable | Description |
|---|---|
| `GOOGLE_OAUTH_CLIENT_ID` | OAuth 2.0 client ID from Google Cloud Console |
| `GOOGLE_OAUTH_CLIENT_SECRET` | OAuth 2.0 client secret |

To create credentials: Google Cloud Console -> APIs & Services -> Credentials -> Create OAuth client ID (Web application). Add `{AUTH_URL}/api/auth/callback/google` as an authorized redirect URI.

### GitHub OAuth

| Variable | Description |
|---|---|
| `GITHUB_OAUTH_CLIENT_ID` | OAuth App client ID from GitHub |
| `GITHUB_OAUTH_CLIENT_SECRET` | OAuth App client secret |

To create credentials: GitHub Settings -> Developer settings -> OAuth Apps -> New OAuth App. Set the authorization callback URL to `{AUTH_URL}/api/auth/callback/github`.

### Microsoft Entra ID

| Variable | Description |
|---|---|
| `MICROSOFT_ENTRA_OAUTH_CLIENT_ID` | Application (client) ID |
| `MICROSOFT_ENTRA_OAUTH_CLIENT_SECRET` | Client secret |
| `MICROSOFT_ENTRA_TENANT_ID` | Directory (tenant) ID. Defaults to `common` (multi-tenant) |

To create credentials: Azure Portal -> App registrations -> New registration. Add `{AUTH_URL}/api/auth/callback/microsoft-entra-id` as a redirect URI (Web platform).

### SSO (OIDC)

For enterprise single sign-on using any OIDC-compliant provider (Okta, Auth0, Keycloak, Azure AD, etc.):

| Variable | Required | Description |
|---|---|---|
| `SSO_PROVIDER_ID` | No | Identifier for the provider (e.g., `okta`). Defaults to `default-sso` |
| `SSO_DOMAIN` | Yes | Email domain to match users to this provider (e.g., `yourcompany.com`) |
| `SSO_ISSUER` | Yes | OIDC issuer URL. Endpoints are auto-discovered via `{SSO_ISSUER}/.well-known/openid-configuration` |
| `SSO_CLIENT_ID` | Yes | OIDC client ID |
| `SSO_CLIENT_SECRET` | Yes | OIDC client secret |

**Okta example:**

1. In Okta Admin -> Applications -> Create App Integration
2. Select OIDC -> OpenID Connect -> Web Application -> Next
3. Set the sign-in redirect URI to `{AUTH_URL}/api/auth/sso/callback/{SSO_PROVIDER_ID}`
4. Copy the Client ID and Client Secret

```env
SSO_PROVIDER_ID="okta"
SSO_DOMAIN="yourcompany.com"
SSO_ISSUER="https://your-org.okta.com"
SSO_CLIENT_ID="<Client ID>"
SSO_CLIENT_SECRET="<Client Secret>"
```

### SSO (SAML)

For enterprise SSO via SAML 2.0:

| Variable | Required | Description |
|---|---|---|
| `SSO_PROVIDER_ID` | No | Identifier for the provider (e.g., `okta`). Defaults to `default-sso` |
| `SSO_DOMAIN` | Yes | Email domain to match users to this provider |
| `SSO_SAML_ENTRY_POINT` | Yes | SAML IdP SSO URL |
| `SSO_SAML_ISSUER` | Yes | The IdP's issuer / entity ID — must match your IdP's SAML issuer (e.g. Okta's **SAML Issuer ID**, `http://www.okta.com/{externalId}`) |
| `SSO_SAML_CERT` | Yes | IdP signing certificate (base64, without BEGIN/END wrapper) |
| `SSO_SAML_CALLBACK_URL` | No | SAML ACS callback URL. Auto-generated if not set |

> **Note:** Only one SSO mode can be active at a time. If `SSO_ISSUER` is set, OIDC takes precedence over SAML.

**Okta example:**

1. In Okta Admin -> Applications -> Create App Integration
2. Select SAML 2.0 -> Next
3. Set Single sign-on URL to `{AUTH_URL}/api/auth/sso/saml2/sp/acs/{SSO_PROVIDER_ID}`
4. Set Audience URI (SP Entity ID) to `{AUTH_URL}`
5. Set Name ID format to `EmailAddress`

```env
SSO_PROVIDER_ID="okta"
SSO_DOMAIN="yourcompany.com"
SSO_SAML_ENTRY_POINT="https://your-org.okta.com/app/xxxxx/sso/saml"
SSO_SAML_ISSUER="http://www.okta.com/{externalId}"
SSO_SAML_CERT="MIIDp..."
SSO_SAML_CALLBACK_URL="{AUTH_URL}/api/auth/sso/saml2/sp/acs/{SSO_PROVIDER_ID}"
```

The user's display name is built from the SAML `firstName` + `lastName` attributes (Okta's default profile attribute names). A custom SAML app sends no attributes by default, so add **Attribute Statements** for them (Name `firstName` → Value `user.firstName`, and likewise `lastName`). For an IdP that uses different attribute names, override with `IDP_FIRSTNAME_CLAIM` / `IDP_LASTNAME_CLAIM` (e.g. `givenName` / `surname`). If absent, the name falls back to the email. The name (and role) re-sync from the IdP on each login.

## Roles & Permissions

Zipline uses role-based access control (RBAC) with three roles:

| Role | Description |
|---|---|
| `admin` | Full access — can manage users, roles, SCIM, plus all operator permissions |
| `operator` | Can cancel/start workflows, deploy schedules, upload/sync configs |
| `viewer` | Read-only access to all data |

The first user to sign up is automatically promoted to `admin`. All subsequent users default to `viewer`. Admins can change roles from the **Admin -> Users** page.

### Permission Matrix

| Action | Admin | Operator | Viewer |
|---|---|---|---|
| View workflows, configs, schedules, metrics | ✅ | ✅ | ✅ |
| Cancel / start workflows | ✅ | ✅ | ❌ |
| Upload / sync configs | ✅ | ✅ | ❌ |
| Deploy / delete schedules | ✅ | ✅ | ❌ |
| Manage user roles | ✅ | ❌ | ❌ |
| Manage SCIM provider connections | ✅ | ❌ | ❌ |

### IdP Role Mapping

When using SSO, roles can be assigned automatically from the IdP:

| Variable | Required | Description |
|---|---|---|
| `IDP_ROLE_MAPPING` | No | Comma-separated mapping of IdP group/claim values to Zipline roles (strict allowlist). Omit for passthrough. |
| `IDP_ROLE_CLAIM` | No | IdP attribute/claim name to read the role from. Defaults to `groups`. |
| `IDP_GROUP_CLAIM` | No | Legacy alias of `IDP_ROLE_CLAIM` (kept for back-compat; `IDP_ROLE_CLAIM` takes precedence). |

**Mapping mode** — `IDP_ROLE_MAPPING` format `idp-group:zipline-role,idp-group2:zipline-role2`:

```env
IDP_ROLE_MAPPING="zipline-admins:admin,zipline-operators:operator"
IDP_ROLE_CLAIM="groups"
```

**Passthrough mode** — omit `IDP_ROLE_MAPPING` and point `IDP_ROLE_CLAIM` at a claim that already carries the role; the value is used directly (no identity mapping needed):

```env
IDP_ROLE_CLAIM="role"
```

If a user belongs to multiple mapped groups, the highest-privilege role wins (`admin` > `operator` > `viewer`). Values that don't resolve to a role are ignored, and users with no matching role keep their existing one.

**Configuring group claims in Okta:**

- **OIDC**: App -> Sign On -> OpenID Connect ID Token -> Add `groups` claim with filter
- **SAML**: App -> Sign On -> Group attribute statements -> Add Name `groups` with filter `Matches regex .*`

`IDP_ROLE_CLAIM` (alias `IDP_GROUP_CLAIM`) is just the SAML/OIDC attribute name to read — it doesn't have to be a group list. Instead of sending every group, you can send a single value via a SAML **Attribute Statement** whose value is an Okta Expression:

- Profile attribute: Name `role`, Value `user.profile.ziplineRole` (a custom attribute added in **Directory -> Profile Editor**), with `IDP_ROLE_CLAIM="role"` and no `IDP_ROLE_MAPPING` (passthrough).
- Computed from groups: Name `groups`, Value `isMemberOfGroupName("Zipline Admins") ? "admin" : "viewer"`.

> Okta Identity Engine references profile attributes as `user.profile.<var>` (the older Classic Engine used `user.<var>`), and the expression must emit one value per role — a comma-joined string is treated as a single literal.

## SCIM (User Provisioning)

Zipline supports [SCIM 2.0](https://scim.cloud/) for automated user provisioning and deprovisioning from identity providers like Okta, Azure AD, and OneLogin.

SCIM allows an IdP to automatically:

- **Create** users when they're assigned to the app in the IdP
- **Update** user attributes (name, email) when they change
- **Deactivate/delete** users when they're removed from the app

### Setup

1. Generate a SCIM token from the Zipline admin UI at `/admin/scim`. The token is shown once and cannot be retrieved again
2. Configure your IdP to use the SCIM base URL: `{AUTH_URL}/api/auth/scim/v2`
3. Provide the generated token as the bearer token in your IdP's SCIM configuration

### Okta SCIM Setup

1. In Okta Admin -> Applications -> select your app
2. Go to Provisioning -> Configure API Integration
3. Check Enable API Integration and enter:
   - **SCIM 2.0 Base URL**: `{AUTH_URL}/api/auth/scim/v2`
   - **OAuth Bearer Token**: The token from the admin UI
4. Click Test API Credentials to verify
5. Under Provisioning -> To App, enable Create Users, Update User Attributes, and Deactivate Users

### Azure AD / Entra ID SCIM Setup

1. In Azure Portal -> Enterprise Applications -> select your app -> Provisioning
2. Set Provisioning Mode to Automatic
3. Under Admin Credentials:
   - **Tenant URL**: `{AUTH_URL}/api/auth/scim/v2`
   - **Secret Token**: The token from the admin UI
4. Click Test Connection to verify
5. Configure attribute mappings and set Provisioning Status to On

## CLI Authentication

The `zipline auth` commands authenticate the CLI via the device authorization flow:

| Command | Description |
|---|---|
| `zipline auth login --url <URL>` | Authenticate via device flow (prompts for URL if omitted) |
| `zipline auth logout` | Clear stored credentials |
| `zipline auth status` | Show current auth state and validate token |
| `zipline auth get-access-token` | Print a short-lived JWT to stdout (for scripts and curl) |

### Usage

```bash
# Log in (opens browser for approval)
zipline auth login --url https://zipline.example.com

# Use the JWT in scripts
curl -H "Authorization: Bearer $(zipline auth get-access-token)" \
  https://zipline.example.com/workflow/v2/list
```

Credentials are stored in `~/.zipline/auth.json`.

### Auth Priority

When making requests to the backend, the CLI tries the following in order:

1. **`ZIPLINE_TOKEN` environment variable** — session token or JWT (see [Token Environment Variable](#token-environment-variable) below)
2. **Hub auth** — session token from `zipline auth login`, exchanged for a short-lived JWT
3. **GCP IAM** — service account via Application Default Credentials
4. **Azure CLI** — DefaultAzureCredential
5. **`ID_TOKEN` environment variable** — fallback for custom setups

### Service Principals

For CI pipelines, automation, and non-interactive environments, admins can create **service principals** — long-lived credentials that don't require browser-based login.

1. Navigate to **Admin -> Service Principals** in the Zipline UI
2. Create a new service principal with a name (e.g., `ci-pipeline`) and a role (`viewer`, `operator`, or `admin`)
3. Copy the generated token — it is shown only once and cannot be retrieved again

The token can be rotated or the service principal deleted from the same admin page.

### Token Environment Variable

The `ZIPLINE_TOKEN` environment variable provides a cloud-agnostic way to authenticate CLI commands without interactive login. It accepts two token types, auto-detected by format:

| Token type | Source | `ZIPLINE_AUTH_URL` required? | Lifetime |
|---|---|---|---|
| **Session token** | Service principal (admin UI) | Yes | Until rotated/deleted |
| **JWT** | `zipline auth get-access-token` | No | ~15 minutes |

**Examples:**

```bash
# CI / automation — service principal token (long-lived, auto-exchanges for JWTs)
export ZIPLINE_TOKEN="<token from admin UI>"
export ZIPLINE_AUTH_URL="https://zipline.example.com"
zipline hub backfill compiled/joins/team/my_join --start-ds=2024-01-01 --end-ds=2024-01-01

# Quick testing — JWT used directly (short-lived, no ZIPLINE_AUTH_URL needed)
ZIPLINE_TOKEN=$(zipline auth get-access-token) zipline hub backfill ...
```

| Variable | Description |
|---|---|
| `ZIPLINE_TOKEN` | A service principal session token or a JWT from `get-access-token` |
| `ZIPLINE_AUTH_URL` | Frontend URL for token exchange (e.g., `https://zipline.example.com`). Required when `ZIPLINE_TOKEN` is a session token |

## Docker Networking

When running backend services in Docker with the frontend running locally (via `--skip-frontend`):

- `AUTH_JWKS_URL` defaults to `http://host.docker.internal:5173/api/auth/jwks`
- Docker compose files include `extra_hosts: ["host.docker.internal:host-gateway"]` for Linux compatibility

When running the full stack in Docker:

- `AUTH_JWKS_URL` defaults to `http://frontend:3000/api/auth/jwks`
