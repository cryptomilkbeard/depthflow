# Agent Notes

This repository is intended to be published as a new GitHub project. Avoid adding any sensitive, identifying, or environment-specific data.

## Local data and secrets
- Do not commit `.env` files or local database files.
- SQLite data lives under `data/` and is ignored by git.
- If you add new local artifacts, update `.gitignore` accordingly.

## Development workflow
- Install deps: `npm install`
- Build: `npm run build`
- Run: `npm start`
- Dev mode: `npm run dev`

## Repo structure
- `src/` TypeScript backend and market data collectors
- `public/` Static dashboard assets
- `data/` Local SQLite storage (ignored)
