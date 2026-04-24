"""
Platform adapter boundary package.

Adapters are the only platform-layer modules allowed to call into product-owned
implementations during the migration period. Long term, these should become
ports/interfaces with injected implementations.
"""
