# Clean-room review checklist

Use this checklist for any PR that adds documentation, dependencies, external material, or feature descriptions.

- [ ] No ASTPP source, schema, asset, archive, or licence file was read or copied.
- [ ] Feature claims cite S1, S2, S3, or S4 from `docs/master-plan.md`.
- [ ] No protocol, schema, API shape, or implementation detail was inferred from proprietary material.
- [ ] New dependencies are FOSS and recorded in `docs/compliance/licence-manifest.json`.
- [ ] Licence changes are reflected in `docs/master-plan.md` section 4.
- [ ] Any new forbidden term or path exception has an owner, reason, and expiry.
- [ ] `make cleanroom` passes locally.
- [ ] CI clean-room report was reviewed when findings appear.
