# Authentication method to use if "method" param is omitted.
DEFAULT_AUTH_METHOD = 'ticket'

# Max string length of the "event" parameter.
MAX_EVENT_LENGTH = 40

# General event errors
ERR_INVALID_EVENT = 'Messages must be JSON and contain an event field.'
ERR_UNHANDLED_EXCEPTION = 'Unhandled exception.'
ERR_EVENT_NOT_FOUND = 'Event not found.'
ERR_SERVICE_UNAVAILABLE = 'Service unavailable.'

# Authentication & authorization
ERR_AUTH_UNSUPPORTED = 'Authentication method unsupported.'
ERR_UNAUTHORIZED = 'Unauthorized.'
ERR_NEEDS_TICKET = 'Must specify ticket.'
ERR_AUTH_FAILED = 'Authentication failed.'
ERR_AUTH_REQUIRED = 'Authentication required.'

# Subscriptions
ERR_INVALID_SUBSCRIPTION_FORMAT = 'Invalid subscription format.'
ERR_INVALID_SERVICE = 'Invalid service.'
ERR_ALREADY_SUBSCRIBED = 'Already subscribed.'
ERR_SUBSCRIPTION_NOT_FOUND = 'Subscription does not exist.'
