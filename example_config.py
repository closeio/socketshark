# Which WebSocket backend to use. Currently only "websockets" is supported.
BACKEND = 'websockets'

# Logging config
LOG = {
    'level': 'INFO',
    'format': '%(message)s'
}


# Host and port to bind WebSockets.
WS_HOST = '0.0.0.0'
WS_PORT = '9000'

# HTTP options when querying services.
HTTP = {
    # Optional path to custom CA file.
    'ssl_cafile': '/path/to/selfsigned.crt',
    'timeout': 15,
    'tries': 3,
    'wait': 3,
}

# Redis options
REDIS = {
    'host': 'localhost',
    'port': 6379,
    'channel_prefix': '',
}

# Authentication (currently only "ticket" authentication is supported)
AUTHENTICATION = {
    'ticket': {
        # API endpoint to validate the ticket and exchange it for auth info.
        'validation_url': 'http://auth-service/auth/ticket/',

        # Fields that the validation endpoint returns.
        'auth_fields': ['session_id', 'user_id'],
    }
}

# List of services
SERVICES = {
    'my_service': {
        # Whether to always require authentication. When False, anonymous
        # sessions are supported even if an authorizer is configured.
        'require_authentication': True,

        # URL to the authorizer which receives auth information (from the
        # authentication endpoint), extra fields (configured below), and
        # subscription information.
        'authorizer': 'http://auth-service/auth/authorizer/',

        # If this service requires extra fields to fulfill a subscription,
        # you may provide them here. They are passed to all URL callbacks.
        'extra_fields': ['organization_id'],

        # If filter fields are specified, messages can be published only to
        # sessions that match the given fields.
        'filter_fields': ['user_id'],

        # Optional URL which is called before subscribing to or unsubscribing
        # from this service. When an error is returned, the subscription or
        # unsubscription command fails.
        "before_subscribe": 'http://my-service/subscribe/',
        "before_unsubscribe": 'http://my-service/unsubscribe/',

        # Optional URL which is called after subscribing to or unsubscribing
        # from this service.
        "on_subscribe": 'http://my-service/on_subscribe/',
        "on_unsubscribe": 'http://my-service/on_unsubscribe/',

        # URL which is called when a message is passed to this service.
        "on_message": 'http://my-service/on_message/',
    },
}
