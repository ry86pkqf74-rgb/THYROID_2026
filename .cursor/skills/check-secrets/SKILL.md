---
name: check-secrets
description: >-
  Scan .toml, .env, and config files for leaked secrets, tokens, or credentials
  before committing. Use before any git add/commit, when editing config files,
  or when the user mentions secrets, tokens, credentials, or .toml files.
---

# Check Secrets

Prevent accidental secret leakage by scanning configuration files before commits.

## When to Run

- Before every `git add` / `git commit`
- After editing `.toml`, `.env`, `.cfg`, or `secrets.*` files
- When creating new config files or connection strings

## Scan Procedure

1. **Find config files** in the workspace:

```bash
# Target file patterns
.streamlit/secrets.toml
.env
.env.*
*.toml (except pyproject.toml, Cargo.toml)
config*.py (look for hardcoded strings)
```

2. **Check each file** for these patterns:

| Pattern | Risk |
|---------|------|
| `TOKEN`, `_TOKEN`, `token =` | API/auth tokens |
| `SECRET`, `_SECRET`, `secret =` | Secret keys |
| `PASSWORD`, `PASS =`, `pwd =` | Passwords |
| `API_KEY`, `apikey`, `api_key` | API keys |
| `eyJ` (base64 JWT prefix) | JWT tokens |
| `sk-`, `pk_`, `sk_` | Stripe/OpenAI keys |
| `ghp_`, `gho_`, `ghs_` | GitHub tokens |
| `md:` or `motherduck_token` | MotherDuck tokens |
| Connection strings with `@` | DB credentials |
| Long base64 strings (>40 chars) | Encoded secrets |

3. **Verify `.gitignore` coverage**:

```
# These MUST be in .gitignore
.streamlit/secrets.toml
.env
*.secrets
credentials.*
```

4. **Check git staging area**:

```bash
git diff --cached --name-only | grep -iE '\.(toml|env|cfg|ini|secrets)$'
```

If any config files are staged, read them and verify no secrets are present.

## Safe Patterns (Allow)

- References to env vars: `os.getenv("MOTHERDUCK_TOKEN")`
- Placeholder values: `your_token_here`, `CHANGEME`, `xxx`
- `toml.load()` calls that read secrets at runtime
- `.gitignore` entries for secret files

## Action on Detection

1. **STOP** — do not commit
2. **Warn the user** with the file path and line number
3. **Suggest remediation**:
   - Move secret to `.streamlit/secrets.toml` or `.env`
   - Add file to `.gitignore`
   - Use `os.getenv()` or `toml.load()` instead of hardcoding
4. **Verify `.gitignore`** covers the secrets file

## Project-Specific Notes

- `.streamlit/secrets.toml` holds `MOTHERDUCK_TOKEN` (467 chars); this file is already in `.gitignore`
- Scripts load the token via `toml.load('.streamlit/secrets.toml')['MOTHERDUCK_TOKEN']` or `os.getenv('MOTHERDUCK_TOKEN')`
- Never print or log token values; use `token[:8]...` for debug output
- The `motherduck_client.py` module handles token loading internally
