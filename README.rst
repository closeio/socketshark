===========
SocketShark
===========

*SocketShark* is a WebSocket message router based on Python/Redis/asyncio.


Features
--------

- Pub-sub messages

  SocketShark allows building applications relying on the publish-subscribe
  pattern without having to be aware of long-running WebSocket connections.
  Subscriptions and published messages from the WebSocket client are forwarded
  to the application via HTTP. Messages can be pushed from the application to
  the WebSocket client synchronously by pushing them to Redis.

- Flexible backend

  SocketShark comes with Websockets for Python 3 (websockets_) backend but can
  easily be adapted to other frameworks compatible with asyncio.

- Multiple services

  Through its configuration file, SocketShark can work with any number of
  services.

- Authentication

  SocketShark comes with ticket authentication built-in. To authenticate
  WebSocket connections, the client requests a temporary token from the app
  server and submits it when logging in. The token is then exchanged for a
  session/user ID that can be used to authenticate the user by the service
  backed by SocketShark.

- Authorization

  Pub-sub subscriptions can be authorized using a custom HTTP endpoint.

- Custom fields

  SocketShark supports custom application-specific fields for authentication
  and authorization purposes.

.. _websockets: https://websockets.readthedocs.io/


Quick start
-----------

See ``example_config.py`` for an example configuration file.

To start, install SocketShark (``python setup.py install``), create your own
configuration file, and run SocketShark as follows:

.. code:: bash
  PYTHONPATH=. socketshark -c my_config
