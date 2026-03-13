class RfxError(Exception):
    """Base application exception with explicit, user-facing diagnostics."""


class ValidationFailure(RfxError):
    """Raised when an input or derived artifact violates a strict contract."""


class ScopeViolation(RfxError):
    """Raised when tenant or case isolation would be breached."""


class ConfigurationFailure(RfxError):
    """Raised when required runtime configuration is missing."""
