# Agent Notes

This repository is intended to be published as a new GitHub project. Avoid adding any sensitive, identifying, or environment-specific data.

## Local data and secrets
- Do not commit `.env` files or local database files.
- SQLite data lives under `data/` and is ignored by git.
- If you add new local artifacts, update `.gitignore` accordingly.
- Do not add API keys, access tokens, or private URLs to source, configs, or docs.

## Configuration defaults
- Defaults are defined in `src/config.ts`. Prefer environment overrides over hardcoding.
- Keep example values generic (no production hosts, accounts, or credentials).

## Development workflow
- Install deps: `npm install`
- Build: `npm run build`
- Run: `npm start`
- Dev mode: `npm run dev`

## Repo structure
- `src/` TypeScript backend and market data collectors
- `public/` Static dashboard assets
- `data/` Local SQLite storage (ignored)

## Data handling
- Metrics and reports are derived from live market data and stored locally in SQLite.
- Keep retention/config values reasonable and avoid bundling any generated data.

## Contribution notes
- Prefer small, focused changes with clear commit messages.
- If adding new endpoints or reports, document them in `README.md`.
- Avoid adding dependencies unless there is a clear need.
