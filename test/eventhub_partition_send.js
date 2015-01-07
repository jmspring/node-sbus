#!/usr/bin/env node
var eventhub  = require("..").eventhub,
    async     = require("async");

var optimist = require('optimist')
    .options('e', { alias : 'eventhub' })
    .options('n', { alias : 'namespace' })
    .options('a', { alias : 'accessuser' })
    .options('p', { alias : 'accesspass' })
    .options('m', { alias : 'message' })
    .options('d', { alias : 'partitionid', default : '0'})    
    .options('g', { alias : 'consumergroup', default : '$default' })
    .options('c', { alias : 'count', default : 1 })
    .boolean('r')
    .demand(['e', 'n', 'a', 'p', 'm'])
    .usage("$0 -e eventhub -n eventhub_namespace -a eventhub_username -p eventhub_password -d partition_id -m message [ -g consumergroup ]")
  ;

var ehnamespace = optimist.argv.namespace;
var ehname = optimist.argv.eventhub;
var access_user = optimist.argv.accessuser;
var access_pass = optimist.argv.accesspass;
var consumer_group = optimist.argv.consumergroup;
var id = optimist.argv.partitionid;
var message = optimist.argv.message;
var count = optimist.argv.count;
var random = optimist.argv.r

var hub = eventhub.EventHub.Instance(ehnamespace, ehname, access_user, access_pass);
if(!hub) {
  console.log("Unable to allocate hub.");
  process.exit(1);
}

function do_work(partition) {
  var i = 0;
  var batch = 100;    // number of messages to send at a time
  var starttime = new Date().getTime();

  function send_messages() {
    var queue = [];
    for(var j = 0; j < batch && i < count; j++, i++) {
      queue.push(function(callback) {
        partition.send(message, null, callback);          
      });
    }        
    async.parallel(queue, function(err, res) {
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

if(!random) {
  hub.getPartition(id, consumer_group, function(err, result) {
    if(err) {
      console.log("Unable to retrieve partition.  Error: " + err);
      process.exit(1);
    } else {
      do_work(result); 
    }
  });
} else {
  hub.getRandomPartition(consumer_group, function(err, result) {
    if(err) {
      console.log("Unable to retrieve random partition.  Error: " + err);
      process.exit(1);
    } else {
      do_work(result); 
    }
  });
}
