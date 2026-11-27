# Security

## Reporting a vulnerability

Please report security problems privately, not in a public issue:

- on GitHub, with [Report a vulnerability](https://github.com/nuggocto/bunpo/security/advisories/new), or
- by email to security@bunpo.org.

Say what you found, how to reproduce it, and which version you ran
(`bunpo --version`). You should get a first reply within a week. Once a fix
is released, the advisory is published with credit to you, unless you
prefer not to be named.

## Supported versions

Only the latest release gets security fixes. Each release is a drop-in
replacement, so update to it rather than waiting for a backport.

## Scope

- the `bunpo` CLI and the GitHub Action (`action.yml`, `scripts/install.sh`)
- `bunpo-server` and the Cloudflare Worker in `worker/`, including the
  dashboard and token handling
- the install scripts served at `bunpo.org/install.sh` and
  `bunpo.org/install.ps1`

The live-queue server runs on your own infrastructure, so how you deploy it
(TLS, where the tokens are stored, who can reach it) is up to you.
[docs/self-hosting.md](docs/self-hosting.md) covers the recommended setup.
