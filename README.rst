===========
SocketShark
===========
.. image:: https://circleci.com/gh/closeio/socketshark/tree/master.svg?style=svg
    :target: https://circleci.com/gh/closeio/socketshark/tree/master

*SocketShark* is a WebSocket message router based on Python/Redis/asyncio.

(Interested in working on projects like this? `Close.io`_ is looking for `great engineers`_ to join our team)

.. _Close.io: http://close.io
.. _great engineers: http://jobs.close.io

.. contents::

Summary
=======

SocketShark makes it easy to build WebSocket-based services without requiring
those services to be aware of WebSockets. Instead, services implement HTTP
endpoints for receiving messages from WebSocket clients, and publish messages
to WebSocket clients via Redis, while SocketShark takes care of long-running
WebSocket connections and passing messages between clients and services.

Features
========

- Pub-sub messages

  SocketShark allows building applications relying on the publish-subscribe
  pattern without having to be aware of long-running WebSocket connections.
  Subscriptions and published messages from the WebSocket client are forwarded
  to the application via HTTP. Messages can be pushed from the application to
  the WebSocket client synchronously by pushing them to Redis.

- Flexible WebSocket backend

  SocketShark comes with Websockets for Python 3 (websockets_) backend but can
  easily be adapted to other frameworks compatible with asyncio.

- Multiple services

  Through its configuration file, SocketShark can work with any number of
  services.

- Out-of-order message filtering

  If needed, an internal order can be supplied with messages from services, and
  SocketShark will automatically filter out out-of-order messages.

- Message throttling

  If needed, service messages can be throttled by SocketShark.

- Authentication

  SocketShark comes with ticket authentication built-in. To authenticate
  WebSocket connections, the client requests a temporary token from the app
  server and submits it when logging in. The token is then exchanged for a
  session/user ID that can be used to authenticate the user by the service
  backed by SocketShark.

- Authorization

  Pub-sub subscriptions can be authorized using a custom HTTP endpoint.
  SocketShark can periodically reauthorize subscriptions to ensure subscribers
  are unsubscribed if they're no longer authorized.

- Custom fields

  SocketShark supports custom application-specific fields for authentication
  and authorization purposes.

- Metrics

  SocketShark keeps track and reports metrics such as connection counts and
  successfully or unsuccessfully executed commands, with built-in Prometheus
  and logging backends.

.. _websockets: https://websockets.readthedocs.io/

Quick start
===========

See ``example_config.py`` for an example configuration file.

To start, install SocketShark (``python setup.py install``), create your own
configuration file, and run SocketShark as follows:

.. code:: bash

  PYTHONPATH=. socketshark -c my_config

Client Protocol
===============

SocketShark uses WebSockets as the transport protocol for clients. This section
describes the structure of the protocol between web clients and SocketShark.

Both clients and the server exchange JSON-messages. Each message is a JSON dict
containing an ``event`` field which specifies the type of event. SocketShark
supports the following events:

- ``auth``: Authentication
- ``subscribe``: Subscribe to a topic
- ``message``: Send a message to a topic
- ``unsubscribe``: Unsubscribe from a topic

Responses usually contain a ``status`` field which can be ``ok`` or ``error``.
In case of an error, an ``error`` field is supplied containing the error
description as a string.

Authentication
--------------

WebSockets clients can authenticate using the ``auth`` event using ticket
authentication. For more information about ticket authentication see the
`Ticket-based authentication for session-based apps`_ section.

The ``auth`` event takes an optional ``method`` (``ticket`` is the only
currently supported authentication method, and the default), and a ``ticket``
argument, containing the login ticket.

Example client request:

.. code:: json

  {"event": "auth", "method": "ticket", "ticket": "SECRET_AUTH_TICKET"}

Example server responses (successful and unsuccessful):

.. code:: json

  {"event": "auth", "status": "ok"}

.. code:: json

  {"event": "auth", "status": "error", "error": "Authentication failed."}

Subscriptions
-------------

WebSocket clients can subscribe to any number of topics. Messages can be passed
from the client to the server, and pushed from the server to the client at any
time while subscribed to a topic. For example, a client may subscribe to an
object ID, and the server may send a message whenever the object is updated.
The server may include extra data when subscribing or unsubscribing. For
example, the server might send the current state of the object when
subscribing.

Topics are unique, and a client can be subscribed to each topic at most once.
Extra fields can be associated with a subscription which are passed along with
all subscription commands. For example, a client could be required to indicate
the organization ID for a particular object subscription so that the service
can authorize and process the message properly.

Subscribe
~~~~~~~~~

The ``subscribe`` event subscribes to a topic given in the ``subscription``
argument, which is composed of the service name and the topic, separated by
period. Extra fields can be defined by the service and directly specified in
the subscription message.

Example client request:

.. code:: json

  {"event": "subscribe", "subscription": "books.book_1"}

Example server responses (successful and unsuccessful):

.. code:: json

  {"event": "subscribe", "subscription": "books.book_1", "status": "ok"}

.. code:: json

  {
    "event": "subscribe",
    "subscription": "books.book_1",
    "status": "error",
    "error": "Book does not exist."
  }

Example server response with extra data:

.. code:: json

  {
    "event": "subscribe",
    "subscription": "books.book_1",
    "status": "ok",
    "data": {
      "title": "Everyone poops"
    }
  }

Example client request with extra fields:

.. code:: json

  {"event": "subscribe", "subscription": "books.book_1", "author_id": "author_1"}

Example successful server responses with extra fields:

.. code:: json

  {
    "event": "subscribe",
    "subscription": "books.book_1",
    "author_id": "author_1",
    "status": "ok"
  }

Note that the subscription name is unique for the subscription. When subscribed
to ``books.book_1`` we can't subscribe to another subcription with the same
name even if the ``author_id`` is different. However, the server could use the
``author_id`` to ensure the book matches the given author ID.

Message
~~~~~~~

Once subscribed, the ``message`` event can be used to pass messages. Message
data is contained in the ``data`` field, and should be dicts. The structure of
the data is up to the application protocol, and the service decides whether
messages are confirmed (successfully or unsuccessfully).

Example message (either client-to-server or server-to-client):

.. code:: json

  {
    "event": "message",
    "subscription": "books.book_1",
    "data": {
       "action": "update",
       "title": "New book title"
    }
  }

Example (optional) server-side message confirmation of a successful message
with extra data:

.. code:: json

  {
    "event": "message",
    "subscription": "books.book_1",
    "status": "ok",
    "data": {"status": "Book was updated."}
  }


Example (optional) server-side message confirmation of a failed message:

.. code:: json

  {
    "event": "message",
    "subscription": "books.book_1",
    "status": "error",
    "error": "Book could not be updated."
  }

If extra fields are passed with the subscription, they are included in all
``message`` events.

Note that a service may send messages limited to particular authentication
fields (e.g. limited to a specific user ID), so multiple sessions subscribed
to the same topic may not necessarily receive the same messages.

Unsubscribe
~~~~~~~~~~~

Clients can unsubscribe from a topic using the ``unsubscribe`` event.

Example client request:

.. code:: json

  {"event": "unsubscribe", "subscription": "books.book_1"}

Example server responses (successful and unsuccessful):

.. code:: json

  {"event": "unsubscribe", "subscription": "books.book_1", "status": "ok"}

.. code:: json

  {
    "event": "unsubscribe",
    "subscription": "books.book_1",
    "status": "error",
    "error": "Subscription does not exist."
  }

Service Protocol
================

SocketShark uses HTTP to send events to services, and Redis PUBSUB to receive
messages from services that are published to subscribed clients. This section
describes the structure of the protocol between services and SocketShark.

HTTP callbacks
--------------

An optional HTTP endpoint can be configured to authenticate a WebSocket
session. The authentication endpoint can return authentication-related fields
that can be configured (e.g. a user ID and/or session ID).

The following optional HTTP endpoints can be configured for each SocketShark
service:

- ``authorizer``: URL to call to authorize a new subscription.
- ``before_subscribe``: URL to call when a client attempts to subscribe.
- ``on_subscribe``: URL to call after a client subscribed to a topic.
- ``on_message``: URL to call when a client sends a message to a topic.
- ``before_unsubscribe``: URL to call when a client attempts to unsubscribe.
- ``on_unsubscribe``: URL to call after a client unsubscribed from a topic.
- ``on_authorization_change``: URL to call after if any authorizer fields
  change during periodic authorization.

Each HTTP endpoint is accessed via a POST request containing a JSON body.

Service-specific endpoints receive any client-supplied extra fields that are
configured for the particular service, as well as authentication-related fields
returned by the authentication endpoint.

HTTP endpoints should return a JSON dict containing a ``status`` field with the
value ``ok`` or ``error``. In case of an error, an error text may be specified
in the ``error`` field.

Authentication
~~~~~~~~~~~~~~

The authentication URL receives JSON dict with the client's ticket supplied in
the ``ticket`` field. Only a successful response authenticates the user.

Example request body:

.. code:: json

  {"ticket": "SECRET_AUTH_TICKET"}

Example server responses (successful with auth fields and unsuccessful):

.. code:: json

  {"status": "ok", "user_id": "user_1", "session_id": "session_1"}

.. code:: json

  {"status": "error", "error": "Authentication failed."}

Authorization
~~~~~~~~~~~~~

If an ``authorizer`` URL is supplied for a service, it is invoked each time a
user attempts to subscribe to a topic. Only a successful response authorizes
the subscription, triggering the ``before_subscribe`` callback (if specified).

If a service has no authorizer, all topics are authorized.

Example request body (for an authenticated session with auth fields as well as
extra client fields):

.. code:: json

  {
    "subscription": "books.book_1",
    "user_id": "user_1",
    "session_id": "session_1",
    "author_id": "author_1"
  }

Example server responses (successful and unsuccessful):

.. code:: json

  {"status": "ok"}

.. code:: json

  {"status": "error", "error": "Author ID does not match book ID."}

During an active subscription, SocketShark will periodically query the
authorizer endpoint if ``authorization_renewal_period`` is set to the number
of seconds. The user will be unsubscribed by SocketShark if the authorization
is no longer valid and an ``unsubscribe`` message will be sent to the client,
e.g.:

.. code:: json

  {
    "event": "unsubscribe",
    "subscription": "books.book_1",
    "error": "Unauthorized."
  }

Before subscribe
~~~~~~~~~~~~~~~~

After a subscription is authorized, the ``before_subscribe`` callback is
invoked with the same arguments as the authorizer. Only a successful response
confirms the subscription, triggering the ``on_subscribe`` callback (if
specified).

Extra data can be returned in this callback using the ``data`` field which is
forwarded to the client. If returned, the ``data`` field should be a dict.

On subscribe
~~~~~~~~~~~~
After a subscription is confirmed, the ``on_subscribe`` callback is invoked
with the same arguments as the authorizer. An unsuccessful response doesn't
affect the client's subscription.

On message
~~~~~~~~~~
When a client sends a message to the service, the ``on_message`` callback is
invoked with the same arguments as the authorizer, plus the message data in the
``data`` field.

A successful response with a ``data`` field, or an unsuccessful response
sends a confirmation to the client.

Example request body (for an authenticated session with auth fields as well as
extra client fields supplied during the subscription):

.. code:: json

  {
    "subscription": "books.book_1",
    "user_id": "user_1",
    "session_id": "session_1",
    "author_id": "author_1",
    "data": {
      "action": "update",
      "title": "New book title"
    }
  }

Example server response (successful, triggers no response):

.. code:: json

  {"status": "ok"}

Example server response (successful, triggers a response):

.. code:: json

  {"status": "ok", "data": {"status": "Book was updated."}

Example server response (unsuccessful, triggers a response):

.. code:: json

  {"status": "error", "error": "Book could not be updated."}

Before unsubscribe
~~~~~~~~~~~~~~~~~~
When a client issues an unsubscribe event, the ``before_unsubscribe`` callback
is invoked with the same arguments as the authorizer. Only a successful
response confirms the unsubscription, triggering the ``on_unsubscribe``
callback (if specified).

Extra data can be returned in this callback using the ``data`` field which is
forwarded to the client. If returned, the ``data`` field should be a dict.

On unsubscribe
~~~~~~~~~~~~~~
After an unsubscription is confirmed, the ``on_unsubscribe`` callback is
invoked with the same arguments as the authorizer. An unsuccessful response
doesn't affect the client's unsubscription.

Publishing messages to clients
------------------------------
To publish a message, a service needs to publish a Redis message to the
appropriate subscription. The message must be JSON-formatted, and contain
the ``subscription`` field, a free-form ``data`` dict and any optional filters
(if the service has configured filter fields). The channel name corresponds to
the subscription (``service.topic``), but a Redis channel prefix may be
optionally configured.

When a filter field is specified, the message is only published to sessions
that match the filter. For example, a message could only be sent to sessions
matching a specific user ID.

Example Redis PUBLISH command:

.. code:: json

  PUBLISH books.book_1 {
    "subscription": "books.book_1",
    "data": {
      "action": "update",
      "title": "New title"
    }
  }

Out-of-order message filtering
------------------------------

Since messages published by services may not necessarily arrive in the desired
order, SocketShark supports message filtering. For example, you might be
publishing updates for a versioned object to Redis but they may arrive
out-of-order due to network latency. Messages can be tagged with an order, and
SocketShark will filter out older messages if a newer message arrives first. A
float order can be supplied both in the `before_subscribe` callback's return
value and in any published message using the `order` option in the `options`
dict. Incoming messages with an order that is lower or equal to the last
received highest order will be filtered out. Multiple independent orders can be
specified using the optional `order_key` option.

In the following example, the "initiating" and "completed" messages, as well as
the "h" and "hello" messages will be delivered to subscribers:

.. code:: json

  PUBLISH calls.call_1 {
    "subscription": "calls.call_1",
    "options": {
        "order": 1,
        "order_key": "call_1.status",
    },
    "data": {
      "status": "initiating",
    }
  }

  PUBLISH calls.call_1 {
    "subscription": "calls.call_1",
    "options": {
        "order": 3,
        "order_key": "call_1.status",
    },
    "data": {
      "status": "completed",
    }
  }

  PUBLISH calls.call_1 {
    "subscription": "calls.call_1",
    "options": {
        "order": 2,
        "order_key": "call_1.status",
    },
    "data": {
      "status": "ringing",
    }
  }

  PUBLISH calls.call_1 {
    "subscription": "calls.call_1",
    "options": {
        "order": 1,
        "order_key": "call_1.note",
    },
    "data": {
      "note": "h",
    }
  }

  PUBLISH calls.call_1 {
    "subscription": "calls.call_1",
    "options": {
        "order": 3,
        "order_key": "call_1.note",
    },
    "data": {
      "note": "hello",
    }
  }

  PUBLISH calls.call_1 {
    "subscription": "calls.call_1",
    "options": {
        "order": 2,
        "order_key": "call_1.note",
    },
    "data": {
      "note": "hell",
    }
  }

Message throttling
------------------

Messages published by services can be throttled by specifying the time in
seconds using the `throttle` option in the `options` dict in the published
message.

For a constant stream of messages that are coming in shorter than the throttle
period, the client will receive the first message immediately, then a message
every throttle period until the stream ends, and then the final message will be
sent after another throttle period elapses.

Multiple independent throttles can be specified using the optional
`throttle_key` option. Throttling is performed per subscription per session.

In the example below, if the three messages are published at the same time, the
first one will be delivered to subscribers immediately, the second one will be
ignored, and the third message will be delivered to subscribers after 100ms
pass.

.. code:: json

  PUBLISH calls.stats {
    "subscription": "calls.stats",
    "options": {
        "throttle" 0.1,
    },
    "data": {
      "n_calls": 1,
    }
  }

  PUBLISH calls.stats {
    "subscription": "calls.stats",
    "options": {
        "throttle" 0.1,
    },
    "data": {
      "n_calls": 2,
    }
  }

  PUBLISH calls.stats {
    "subscription": "calls.stats",
    "options": {
        "throttle" 0.1,
    },
    "data": {
      "n_calls": 3,
    }
  }


Use patterns
============

This section illustrates how to implement common use patterns when building a
service with SocketShark.

Ticket-based authentication for session-based apps
--------------------------------------------------

Most web applications use an HTTP-only cookie that stores a session ID for
authentication. Since WebSocket connections are initiated via JavaScript, there
is no access to the session ID via the cookie. To facilitate authentication of
WebSocket connections, authentication with single-use tickets should be used:

- Implement a public "ticket" endpoint in your application. The endpoint should
  validate the user's session and return a random-generated short-lived ticket
  associated to the user's session ID. For example, a UUID4 ticket may be
  computed and stored in Redis with a 30 second expiration using the SETEX
  command, where the key name corresponds to the ticket (the UUID4), and the
  key value is the user's session ID.

- Implement an internal ticket validation in your application. The endpoint
  should be configured as the auth endpoint in SocketShark. It should retrieve
  and return the user's session ID, and at the same time invalidate the ticket.
  Any other user information (e.g. user ID) may also be returned. A Redis
  pipeline should be used to retrieve and delete the ticket.

- When the JavaScript code connects to SocketShark, it should first request a
  ticket via the public ticket endpoint, then connect to SocketShark and issue
  the authentication event with the obtained ticket.

Authorization with microservices
--------------------------------

Suppose a user can access products from a set of authorized organization IDs.
The auth service stores a list of users and corresponding organization IDs that
users have access to. The product service stores a list of products with
corresponding organization IDs but is not aware whether a user is authorized to
access a specific organization (and therefore product). Subscriptions are per
product and should only be authorized if the user can access the product's
organization. To solve this problem without requiring the services to directly
talk to each other, extra fields can be used in SocketShark:

- Add the user ID in SocketShark's authentication configuration under
  ``auth_fields``, and the organization ID as under the product service's
  ``extra_fields``.

- Return the user ID in the auth service's authorization endpoint. SocketShark
  will supply it in all subsequent requests to service endpoints.

- When the client subscribes to the product service (subscription example:
  ``product.PROD_ID``), it must also supply the product's organization ID as an
  extra field.

- Set up an ``authorizer`` URL for the product service that points to the auth
  service. The auth service should authorize a subscription if the given user
  has access to the given organization. Since the authorizer doesn't have
  access to the product database, it doesn't validate the product ID.

- Set up a ``before_subscribe`` URL for the product service that points to the
  product service. The product service should allow a subscription if the
  subscription's product ID matches the given organization ID. Since the
  organization ID is already validated by the authorizer, no further validation
  is necessary.
