# validation-reference workflow package

This package includes the exact version 1.0 definition, trusted content manifest,
locked runtime/executor dependencies and all named executable Activity handlers.
It requires no external capability workers, API package or root distribution.
The reference handlers use mock adapters; real enterprise services are later work.

Build complete artifacts from the repository root:

```text
uv run --locked python scripts/build_workflow_packages.py --verify
```

The resulting platform-specific wheelhouse ZIP contains the full executable
closure. Its immutable release descriptor is outside the ZIP and binds the
canonical manifest hash to the final artifact SHA-256; the manifest contains no
self-referential artifact digest. A content/code change requires a new package
version/Build ID and release rather than altering an existing serving artifact.

From the repository root, expand the verified ZIP into `release/`, then install
without a package index. Copy operator pool configuration separately; it is not
inside the release artifact:

```text
uv venv .package-env
uv pip install --python .package-env/Scripts/python.exe --no-index --find-links release/wheelhouse nexusflow-validation-reference-package==0.2.0
Copy-Item tests/fixtures/workflow-packages/validation-reference/pools/mixed.json operator-pool.json
.package-env/Scripts/python.exe -m workflow_executor --package validation-reference --pool operator-pool.json
```

On Unix use `.package-env/bin/python` and `cp` instead of `Copy-Item`. After
installation and supplying the pool document, execution requires no checkout.
See [local setup](../../../../docs/development/workflow-packages.md) for release
admission.

The example pool files are desired local configuration, not observed readiness.
Each invocation creates one replica; start additional processes using different
probe ports to reach the desired count. Split roles use workflow.json and
activity.json with the same stable queues, artifact, deployment name and Build ID.
Do not change queue bindings for open executions. Serving/ramping version routing
must be approved using Temporal Worker Deployment operations before API starts.
The second validation-reference package proves independent package ownership.
