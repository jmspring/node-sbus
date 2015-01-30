Introduction
============

A library for connecting to Azure Service Bus services via AMQP.

Currently, only Event Hub is supported and can be accessed by one of two methods:

1. By individual partition
2. By the entire eventhub (referred to as Event Processor)

The library operates by taking an AMQP provider and wrapping it in higher-level operations and connection management
to make it easy to run against Azure EventHub.  It can store state into Azure Table Storage as well, providing the seamless
ability to continue receiving messages from where you left off - akin to the .NET EventProcessorHost.

AMQP Processor Requirements
===========================

`node-sbus` relies on five simple methods to provide AMQP support - two for service bus, two for event hub, one for teardown:

* `send(uri, payload, cb)`
  * The URI should be the full AMQPS address you want to deliver to with included SAS name and key,
    e.g. amqps://sasName:sasKey@sbhost.servicebus.windows.net/myqueue.
  * The payload is a JSON payload (which might get `JSON.stringify`'d), or a string.
  * The callback takes an error, and is called when the message is sent.
* `receive(uri, cb)`
  * The URI should be the full AMQP(S) address you want to receive from, e.g. amqps://sasName:sasKey@sbhost.servicebus.windows.net/mytopic/Subscriptions/mysub.
  * The callback takes an error, a message payload, and any annotations on the message, and is called every time a message
    is received.
* `eventHubSend(uri, payload, [partitionKey], cb)`
  * The URI should be the full AMQPS address of the hub with included SAS name and key,
    e.g. amqps://sasName:sasKey@sbhost.servicebus.windows.net/myeventhub
  * The payload is a JSON payload (which might get `JSON.stringify`'d), or a string.
  * The (optional) partition key is a string that gets set as the partition key for the message (delivered in the
    message annotations).
  * The callback takes an error, and is called when the message is sent.
* `eventHubReceive(uri, [offset], cb)`
  * The URI should be the AMQPS address of the hub with included SAS name and key, consumer group suffix and partition,
    e.g. amqps://sasName:sasKey@sbhost.servicebus.windows.net/myeventhub/ConsumerGroups/_groupname_/Partition/_partition_
  * The (optional) offset should be the string offset provided by the message annotations of received messages, allowing
    connections to pick up receipt from where they left off.
  * The callback takes an error, a partition ID, a message payload, and any annotations on the message, and is called every time a message
    is received.
* `disconnect(cb)`
  * Disconnect from all open links and tear down the connection.

Any class implementing these five methods is duck-type compatible with `node-sbus` and can be used.


