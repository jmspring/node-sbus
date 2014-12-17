#!/usr/bin/env node
var eventhub = require("..").eventhub;

var optimist = require('optimist')
    .options('e', { alias : 'eventhub' })
    .options('n', { alias : 'namespace' })
    .options('a', { alias : 'accessuser' })
    .options('p', { alias : 'accesspass' })
    .options('g', { alias : 'consumergroup', default : '$default' })
    .options('s', { alias : 'storagetable', default : null })
    .options('k', { alias : 'storagekey', default : null })
    .demand(['e', 'n', 'a', 'p'])
    .usage("$0 -e eventhub -n eventhub_namespace -a eventhub_username -p eventhub_password [ -g consumergroup ] [ -s storagetable -k storagekey ]")
  ;

var ehnamespace = optimist.argv.namespace;
var ehname = optimist.argv.eventhub;
var access_user = optimist.argv.accessuser;
var access_pass = optimist.argv.accesspass;
var consumer_group = optimist.argv.consumergroup;

var hub = eventhub.EventHub.Instance(ehnamespace, ehname, access_user, access_pass);
if(!hub) {
  console.log("Unable to allocate hub.");
  process.exit(1);
}

function subscribe_callback(err, result) {
  if(err) {
    console.log("Failure subscribing.  Error: " + err);
    process.exit(1);
  }
  
  console.log("subscribed: " + result);
}

function receive_callback(err, result) {
  if(err) {
    console.log("Error receiving message.  Error: " + err);
  } else {
    console.log("Message:");
    console.log("  body: " + JSON.stringify(result.message.body));
    if("annotations" in result.message) {
      console.log("  annotations: " + JSON.stringify(result.message.annotations));
    }
    if("properties" in result.message) {
      console.log("  properties: " + JSON.stringify(result.message.properties));
    }
  }
}

hub.getEventProcessor(consumer_group, function(err, result) {
  var processor = result;
  if(err) {
    console.log("Unable to allocate event processor.  Error: " + err);
    process.exit(1);
  }

  processor.init(subscribe_callback, receive_callback, function(err, result) {
    if(err) {
      console.log("Unable to subscribe.  Error: " + err);
      process.exit(1);
    } else {
      if(optimist.argv.storagetable && optimist.argv.storagekey) {
        processor.set_storage(optimist.argv.storagetable, optimist.argv.storagekey);
      }
      processor.receive();
    }
  });
});
