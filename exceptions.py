class ExponentialBackoffFailed(Exception):
    """Exponential Backoff Failed"""
    pass


class MissingEnvironmentVariable(KeyError):
    """Missing Environment Variable"""
    pass
