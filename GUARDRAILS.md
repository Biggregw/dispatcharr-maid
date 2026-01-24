# Dispatcharr-Maid Guardrails

This project is intentionally based on commit:

4c51c6b7b24e44d3c0143ad13395a115380608e7

The following functionality is **explicitly forbidden**:
- windowed runners
- background optimisation runners
- auto-invoked CLI batch tools
- implicit scheduling

Any new work must:
- preserve the existing execution model
- require explicit user invocation
- avoid background or timed execution

If a change reintroduces any of the above, it is a regression.
