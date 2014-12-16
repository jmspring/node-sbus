#!/usr/bin/env node
var eventhub  = require("..").eventhub,
    async     = require("async");

var optimist = require('optimist')
    .options('e', { alias : 'eventhub' })
    .options('n', { alias : 'namespace' })
    .options('a', { alias : 'accessuser' })
    .options('p', { alias : 'accesspass' })
    .options('g', { alias : 'consumergroup', default : '$default' })
    .options('m', { alias : 'message' })
    .options('c', { alias : 'count', default : 1 })
    .demand(['e', 'n', 'a', 'p', 'm'])
    .usage("$0 -e eventhub -n eventhub_namespace -a eventhub_username -p eventhub_password -m message [ -g consumergroup ] [ -c count ]")
  ;

var ehnamespace = optimist.argv.namespace;
var ehname = optimist.argv.eventhub;
var access_user = optimist.argv.accessuser;
var access_pass = optimist.argv.accesspass;
var consumer_group = optimist.argv.consumergroup;
var count = optimist.argv.count;
var message = optimist.argv.message;

var hub = eventhub.EventHub.Instance(ehnamespace, ehname, access_user, access_pass);
if(!hub) {
  console.log("Unable to allocate hub.");
  process.exit(1);
}

hub.getEventProcessor(consumer_group, function(err, result) {
  var processor = result;
  if(err) {
    console.log("Unable to allocate event processor.  Error: " + err);
    process.exit(1);
  }
  processor.init(null, null, function(err, result) {
    if(err) {
      console.log("error initializing: " + err);
      process.exit(1);
    } else {
      var i = 0;
      var batch = 4;    // number of messages to send in parallel
      var starttime = new Date().getTime();


      function send_messages() {
        var queue = [];
        for(var j = 0; j < batch && i < count; j++, i++) {
          queue.push(function(callback) {
            processor.send(message, null, callback);          
          });
        }        
        async.series(queue, function(err, res) {
          if(err) {
            console.log("Error occurred: " + err);
          } else {
            if(i < count) {
              send_messages();
            } else {
              var stoptime = new Date().getTime();
              console.log("Message(s) sent.  Count: " + i + ", Total time: " + (stoptime - starttime) + "ms");
            }
          }
        });
      }
      
      send_messages();
    }
  });
});
