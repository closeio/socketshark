from .types import (
    AuthConfig,
    HttpOptions,
    LogConfig,
    MetricsProviderConfig,
    RedisSettings,
    ServiceConfig,
    WsPingConfig,
    WsSslConfig,
)

# Default settings that can be overridden.

# Which WebSocket backend to use. Currently only "websockets" is supported.

BACKEND = "websockets"

# Logging config
LOG: LogConfig = LogConfig(
    {
        "setup_structlog": True,
        "level": "INFO",  # Set to None to disable logging setup
        "format": "%(message)s",
        "logger_name": "socketshark",
        # Trace loggers are prefixed with the value below (separated by dot).
        "trace_logger_prefix": "trace",
        # Set to 'DEBUG' to enable trace logger, or 'INFO' or higher to disable.
        "trace_level": "INFO",
    }
)

METRICS: dict[str, MetricsProviderConfig] = {
    # Set a port to enable Prometheus integration
    # 'prometheus': {
    #     'host': '',
    #     'port': None,
    # },
    # Log all metrics
    # 'log': {},
}

# Host and port to bind WebSockets.
WS_HOST = "127.0.0.1"
WS_PORT = "9000"
WS_SSL: WsSslConfig = WsSslConfig(
    {
        # 'cert': '/path/to/ssl.crt',
        # 'key': '/path/to/ssl.key',
    }
)

WS_PING: WsPingConfig = WsPingConfig(
    {
        # How often to ping WebSocket connections in seconds (None to not ping).
        "interval": 15,
        # Seconds after which we disconnect clients with no ping response.
        "timeout": 15,
    }
)

# HTTP options when querying services.
HTTP: HttpOptions = HttpOptions(
    {
        # Optional path to custom CA file.
        "ssl_cafile": None,
        "timeout": 15,
        "tries": 3,
        "wait": 3,
        # If we encounter HTTP 429, wait for the amount of seconds specified in
        # the following header (fall back to the 'wait' value if it doesn't exist).
        "rate_limit_reset_header_name": "X-Rate-Limit-Reset",  # Deprecated, use rate_limit_reset_header_names instead.
        "rate_limit_reset_header_names": [],  # List of header names to check for the rate limit reset time.
    }
)

# Redis options
REDIS: RedisSettings = RedisSettings(
    {
        "host": "localhost",
        "port": 6379,
        "channel_prefix": "",
        # How often to ping Redis in seconds (None to not ping).
        "ping_interval": 10,
        # Seconds after which we shut down after no ping response.
        "ping_timeout": 5,
    }
)

# Alternative Redis configuration. Useful for migration.
# If set, every topic will subscribe to both Redis instances.
# A typical migration scenario would be to set up a new Redis instance
# and set as REDIS, keeping the old Redis instance as REDIS_ALT.
# Then update any publishers to publish to the new instance.
# Once all publishers have been updated, set REDIS_ALT to None.
REDIS_ALT: RedisSettings | None = None

# Authentication
AUTHENTICATION: AuthConfig = AuthConfig({})

# List of services
SERVICES: dict[str, ServiceConfig] = {}
