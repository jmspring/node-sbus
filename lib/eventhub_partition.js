var qpid        = require('qpid'),
    request     = require('request'),
    storage     = require('./storage'),
    eventhub    = require('./eventhub'),
    eventhubutils = require('./eventhubutils');
    builder     = require("node-amqp-encoder").Builder;
    
function PartitionInstance(eventhub, group, id, messenger, subscribed)
{
  if(!subscribed || !messenger) {
    subscribed = false;
  }
  
  var instance = {
    // info
    eventhub:         eventhub,
    group:            group,
    id:               id,
    
    // storage
    storage:          {
      name:           null,
      key:            null,
      instance:       null,
      updater:        null
    },
    
    // communications layer and callbacks
    send_receive:     {
      messenger:      (messenger === null) ? new qpid.proton.Messenger() : messenger,
      subscribeuri:   eventhub.config.baseamqpuri + "ConsumerGroups/" + group + "/Partitions/" + id,
      messageuri:     eventhub.config.baseamqpuri + "Partitions/" + id,
      subscribed:     subscribed,    
      receivecb:      null
    },

    set_storage: function(name, key, interval) {
      instance.storage.name = name;
      instance.storage.key = key;
      instance.storage.instance = storage.azure_store(name, key);
      if(!interval) {
        interval = eventhubutils.DEFAULT_STORAGE_INTERVAL;
      }
      if(interval > 0) {
        instance.storage.updater = setInterval(function() {
          instance.update_partition_state();
        }, interval);
      } else {
        if(instance.storage.updater !== null) {
          clearInterval(instance.storage.updater);
          instance.storage.updater = null;
        }
      }
    },
    
    set_receive_callback: function(callback) {
      instance.send_receive.receivecb = callback;
    },
    
    update_partition_state: function(callback) {
      if(instance.storage.instance) {
        var state = instance.eventhub.get_partition_state(instance.group, instance.id);
        if(state) {
          var s = {};
          s[instance.id] = { id: instance.id, state: state };
          instance.storage.instance.store_eventhub_state(instance.eventhub.config.space,
                                                         instance.eventhub.config.name,
                                                         instance.group,
                                                         s,
                                                         function(result) {
            if(callback) {
              callback(result);
            }
          });
        } else {
          if(callback) {
            callback(result);
          }
        }
      } else {
        if(callback) {
          callback(result);
        }
      }
    },
   
    subscribe: function(callback) {
      var filter = [];
      var partitionInfo = instance.eventhub.runtime.partitions[instance.group][instance.id]; 
      
      function do_subscribe() {
        if(("state" in partitionInfo) && ("x-opt-offset" in partitionInfo.state)) {
          var b = new builder();
          filter = b.map().
                     symbol("apache.org:selector-filter:string").
                     described().symbol("apache.org:selector-filter:string").
                                 string("amqp.annotation.x-opt-offset > '" + partitionInfo.state["x-opt-offset"] + "'").
                     end().                    
                     encode(); 
        }
        instance.send_receive.messenger.subscribe(instance.send_receive.subscribeuri, { sourceFilter: filter }, function(result) {
          instance.send_receive.subscribed = true;
          callback(null, result);
        });
      }
      
      if(instance.storage.instance && !("state" in partitionInfo)) {
        // if we have storage set and no state retrieved for the partition, we need to get it
        instance.storage.instance.retrieve_eventhub_state(instance.eventhub.config.space,
                                                          instance.eventhub.config.name,
                                                          instance.group,
                                                          [ partitionInfo ],
                                                          function(result) {
          if(result) {
            var found = false;
            for(var i = 0; i < result.length; i++) {
              var id = result[i].id;
              if(id == instance.id) {
                instance.eventhub.set_partition_state(instance.group, instance.id, result[i].state);
                found = true;
                break;
              }
            }
            if(!found) {
              callback("unable to retrieve state");
            } else {
              do_subscribe();
            }
          } else {
            callback("unable to retrieve partition info");
          }
        });
      } else {
        do_subscribe();
      }
    },

    // calls subscribe if needed
    receive: function(callback) {
      // if a callback is specified, we register it locally
      if(callback) {
        instance.send_receive.receivecb = callback;
      }
      
      instance.send_receive.messenger.on('message', function(message, subscription) {
        var partitionId = subscription.substring(subscription.lastIndexOf("/") + 1);
        if(partitionId != instance.id) {
          // TODO -- received a message not meant for this partition
        } else {
          if(message.annotations && (message.annotations.length == 2)) {
            var annotationMap = message.annotations[1];
            for(var i = 0; i < annotationMap.length / 2; i++) {
              if(annotationMap[2 * i][1] == 'x-opt-offset') {
                instance.eventhub.update_partition_state(instance.group, instance.id, 'x-opt-offset', annotationMap[2 * i + 1][1]);
                break;
              }
            }
          }

          eventhubutils.parse_message_from_qpid(message);
          if(instance.send_receive.receivecb) {
            instance.send_receive.receivecb(null, { message: message, subscription: subscription });
          }
        }
      });
      
      if(!instance.send_receive.subscribed) {
        instance.subscribe(function(result) {
          // TODO -- add error handling
          instance.send_receive.messenger.receive();
        });
      } else {
        instance.send_receive.messenger.receive();
      }
    },
    
    send: function(body, key, callback) {
      if(!instance.send_receive.messenger) {
        callback("messenger not setup");
      } else if(key) {
        callback("specifying partition key not supported for specific partition");
      } else {
        var message = {
          content:    body,
          encoding:   "json"
        };
        instance.send_receive.messenger.send( { address: instance.send_receive.messageuri,
                                                body: JSON.stringify(message) },
                                                function(err, msg) {
          if(callback) {
            callback(err);
          }
        });
      }
    }
  };
  
  return instance;
}

module.exports.Instance = PartitionInstance;
