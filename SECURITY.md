# Security Policy

## Supported Versions

We support the latest published version on crates.io.

## Reporting a Vulnerability

The preferred way to report a security vulnerability is through GitHub's private vulnerability reporting feature:

1. Go to the repository's **Security** tab.
2. Click **"Report a vulnerability"**.
3. Provide as much detail as possible (description, reproduction steps, impact, affected versions).

**Alternative contact**: Open a private discussion with the maintainer via GitHub or email the address listed in the repository owner profile.

We will:
- Acknowledge receipt of your report within **5 business days**.
- Work with you to understand and resolve the issue.
- Credit you in the release notes / advisory (unless you prefer to remain anonymous).

## Disclosure Policy

- We follow **coordinated responsible disclosure**.
- Please allow **90 days** from the initial report before public disclosure.
- We aim to release patches for confirmed vulnerabilities as quickly as possible.

## Scope

This policy covers the source code, documentation, and configuration files in **this repository** (`tower-sessions-stores`).

**Out of scope**:
- Issues in third-party dependencies (report to their respective projects)
- The main `tower-sessions` crate (see its own security policy at https://github.com/maxcountryman/tower-sessions)
- Social engineering or physical attacks

## Bug Bounty

This project does not currently run a paid bug bounty program. We are grateful for responsible security research and will publicly acknowledge high-quality reports.

Thank you for helping keep the Rust web ecosystem secure!
