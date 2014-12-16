node-sbus
=========

A library for connecting to Azure Service Bus services.

Currently, only Event Hub is supported and can be accessed by one of two methods:

1. By individual partition
2. By the entire eventhub (referred to as Event Processor)

The examples in the "test" directory give a general approach on how to use each method.

This module relies on a version of [node-qpid](https://github.com/jmspring/node-qpid) which
is a fork of the original module.  As pull requests are made into the [original node-qpid]
(https://github.com/pofallon/node-qpid) this requirement will change.
