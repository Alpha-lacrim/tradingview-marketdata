# FastAPI v1.1.0rc1 — company release candidate

First installation of a dedicated market-data service. v1.0 is NOT a prerequisite. This
candidate is not production approval. Company/site qualification and the full genuine 99-feed
universe remain open. Reviewed M4 established a brief genuine BTC dual-source check and bounded
injected 99-feed correctness/performance; neither is a sustained all-provider guarantee.

The one application image runs separate READ and INGEST processes. No collector, concrete
TradingView transport, source schema, worker, TradingView credential, private data or backup is
included. READ has a SELECT-only DSN and read service key; INGEST has a restricted writer DSN
and producer fingerprints. Neither receives schema-admin credentials. The unrelated company
application databases remain separate. The collector only needs source DB credentials and HTTPS.

See [company installation](INSTALL-COMPANY.md), [operations/restore](OPERATIONS.md), and
[controlled transition](COLLECTOR-TRANSITION.md). The separate collector/worker archive is for
the collection operator. Source track is 0006+sc0002; company track is 0010. Source sc0001/sc0002
must never be installed here. Historical implementation docs are context; use these release
installation commands. RELEASE.json identifies the uncommitted reviewed source and base image;
SHA256SUMS covers every payload file except itself. Verify the outer archive against the
separately supplied checksum before extracting and running package scripts.

Build with the exact PYTHON_IMAGE in RELEASE.json:

```bash
uv sync --frozen --dev
uv run pytest -q tests
python -c "import json;print(json.load(open('RELEASE.json'))['python_image'])"
docker build --no-cache --build-arg PYTHON_IMAGE=REPLACE_WITH_EXACT_RELEASE_IMAGE -t marketdata-fastapi:1.1.0rc1 .
```

PowerShell uses the same commands, replacing the build argument with
`--build-arg "PYTHON_IMAGE=$((Get-Content RELEASE.json | ConvertFrom-Json).python_image)"`.
Linux can use `--build-arg "PYTHON_IMAGE=$(python3 -c 'import json;print(json.load(open("RELEASE.json"))["python_image"])')"`.
A fresh network build installs hashed requirements.lock without a wheel cache or host .venv.
uv==0.11.27 is the reference host tool. The wheel/source build backend is hatchling==1.27.0.
Do not substitute a cache-only build as clean network verification; see the M5 sidecar report
for what was actually executed. Both Windows Python and Linux Docker paths use these locked
files; runtime Python source matches the collector package's shared modules byte-for-byte.

Before installing, run `python scripts/verify_payload.py` from the extracted root, after checking
the outer archive hash from the supplied sidecar. For restricted package networks, Docker accepts
`--build-arg PACKAGE_INDEX=https://APPROVED_HTTPS_INDEX/simple/`. All runtime wheels must match
requirements.lock hashes. Never use --trusted-host, disable TLS, or put repository passwords in
build arguments. A public mirror may be used only if allowed by company policy; the candidate
verification sidecar records the index actually tested and default-index failures separately.
