from typing import Any, NewType

# Config types
Config = NewType("Config", dict[str, Any])
ServiceConfig = NewType("ServiceConfig", dict[str, Any])
AuthConfig = NewType("AuthConfig", dict[str, Any])
RedisSettings = NewType("RedisSettings", dict[str, Any])
HttpOptions = NewType("HttpOptions", dict[str, Any])
LogConfig = NewType("LogConfig", dict[str, Any])
WsPingConfig = NewType("WsPingConfig", dict[str, Any])
WsSslConfig = NewType("WsSslConfig", dict[str, Any])
MetricsProviderConfig = NewType("MetricsProviderConfig", dict[str, Any])

# Protocol types
ClientEventData = NewType("ClientEventData", dict[str, Any])
ServiceEventData = NewType("ServiceEventData", dict[str, Any])
ClientMessage = NewType("ClientMessage", dict[str, Any])
ServiceRequestData = NewType("ServiceRequestData", dict[str, Any])
ServiceResponse = NewType("ServiceResponse", dict[str, Any])

# Error types
EventErrorData = NewType("EventErrorData", dict[str, Any])

# Domain types
SubscriptionName = NewType("SubscriptionName", str)

# Session-scoped types
AuthInfo = NewType("AuthInfo", dict[str, Any])
ExtraData = NewType("ExtraData", dict[str, Any])
SessionInfo = NewType("SessionInfo", dict[str, Any])
MessageOptions = NewType("MessageOptions", dict[str, Any])
