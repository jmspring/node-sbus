#!/usr/bin/env node
var eventhub  = require("..").eventhub,
    async     = require("async");

var optimist = require('optimist')
    .options('e', { alias : 'eventhub' })
    .options('n', { alias : 'namespace' })
    .options('a', { alias : 'accessuser' })
    .options('p', { alias : 'accesspass' })
    .options('m', { alias : 'message' })
    .options('d', { alias : 'partitionid', default : null})    
    .options('g', { alias : 'consumergroup', default : '$default' })
    .options('c', { alias : 'count', default : 1 })
    .options('k', { alias : 'partitionkey', default : null })
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
var partition_key = optimist.argv.partitionkey;

if((id != null) && (partition_key != null)) {
  console.log("Either 'partitionid' or 'partitionkey' can be specified, not both");
  process.exit(1);
} else if((partition_key == null) && (id == null)) {
  id = "0";     // default to partition id 0
}

var hub = eventhub.EventHub.Instance(ehnamespace, ehname, access_user, access_pass);
if(!hub) {
  console.log("Unable to allocate hub.");
  process.exit(1);
}

function do_work(partition) {
  var i = 0;
  var sent = 0;
  var batch = 100;    // number of messages to send at a time
  var starttime = new Date().getTime();

  function send_messages() {
    var queue = [];
    for(var j = 0; j < batch && i < count; j++, i++) {
      queue.push(function(callback) {
      try {
        partition.send(message, partition_key, function(err, res) {
          if(err) {
            console.log(err)
          }
          callback(err);
        });
      } catch(err) {
        console.log("Send fail");
        console.log(err);
      }          
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

if(partition_key == null) {
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
} else {
  // sending based on partition_key
  hub.getEventProcessor(consumer_group, function(err, result) {
    if(err) {
      console.log("Unable to retrieve event processor for sending to partition key.  Error: " + err);
      process.exit(1);
    } else {
      result.init(null, null, function(err) {
        if(err) {
          console.log("Unable to init event processor.  Error: " + err);
          process.exit(1);
        } else {
          do_work(result); 
        }
      });
    }  
  });
}
