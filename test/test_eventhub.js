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

var eh = eventhub.create_instance(ehnamespace, ehname, access_user, access_pass);

var ehp = eh.event_processor_instance(consumer_group, function(message, sub) {
            console.log("subscription -- " + sub);
            if(message.body) {
              console.log(message.body);
            }
            if(message.properties) {
              console.log(message.properties);
            }
            if(message.annotations) {
              console.log(message.annotations);
            }
          });
          
// indicate which partitions were subscribed to
ehp.subscribe_handler(function(url) {
  console.log("subscribed to -- " + url);
});

// if specified, add a storage account
if(optimist.argv.storagetable && optimist.argv.storagekey) {
  ehp.add_store(optimist.argv.storagetable, optimist.argv.storagekey);
}

ehp.process();

// show state of event processor every 30 seconds
setInterval(function() {
  var state = ehp.state();
  console.log(JSON.stringify(state, null, 2));
}, 30000);
