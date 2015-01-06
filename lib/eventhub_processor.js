var async       = require('async'),
    qpid        = require('qpid'),
    request     = require('request')
    storage     = require('./storage'),
    eventhub    = require('./eventhub'),
    eventhubutils = require('./eventhubutils');
    
function EventProcessorInstance(eventhub, group) {
  if(group == null) {
    group = "$Default";
  }
  
  var sendKey = 0;
  
  var instance = {
    // info
    eventhub:       eventhub,
    group:          group,
    
    // storage
    storage:     {
      name:         null,
      key:          null,
      instance:     null,
      updater:      null
    },
    
    send_receive: {
      messenger:          new qpid.proton.Messenger(),
      subscribebaseuri:   eventhub.config.baseamqpuri + "ConsumerGroups/" + group + "/Partitions/",
      senduri:            eventhub.config.baseamqpuri,
      receivecb:          null,
      subscribecb:        null
    },
    
    runtime: {
      initialized:        false,
      subscribed:         false,
      partitions:         {}
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
          instance.update_hub_state();
        }, interval);
      } else {
        if(instance.storage.updater != null) {
          clearInterval(instance.storage.updater);
          instance.storage.updater = null;
        }
      }
    },
    
    update_hub_state: function(callback) {
      if(instance.storage.instance) {
        var partitions = {};
        Object.keys(instance.runtime.partitions).forEach(function(id) {
          state = instance.eventhub.get_partition_state(instance.group, id);
          // we only want to update the state when there is something to update
          if(Object.keys(state).length > 0) {
            partitions[id] = {
              id:     id,
              state:  state
            };
          }
        });
        if(Object.keys(partitions).length > 0) {
          instance.storage.instance.store_eventhub_state(instance.eventhub.config.space,
                                                         instance.eventhub.config.name,
                                                         instance.group,
                                                         partitions,
                                                         function(result) {
            if(callback) {
              callback(result);
            }
          });
        } else {
          if(callback) {
            callback(null);
          }
        }
      }
    },
    
    subscribe: function(callback) {
      if(!instance.runtime.initialized) {
        callback("event processor not initialized");
      } else {
        var keys = Object.keys(instance.runtime.partitions);
        var tasks = [];
        var subresult = {};
        keys.forEach(function(key) {
          tasks.push(function(cb) {      
            instance.runtime.partitions[key].subscribe(function(err, result) {
              if(err) {
                cb("subscribe failed: " + err);
              } else {
                if(instance.send_receive.subscribecb) {
                  instance.send_receive.subscribecb(null, result);
                }
                if(result == null) {
                  cb("failed to subscribe to: " + key);
                } else {
                  cb(null, result);
                }
              }
            });
          });
        });
        async.series(tasks, function(err, result) {
          if(err) {
            callback(err);
          } else {
            instance.runtime.subscribed = true;
            callback(null);
          }
        });
      }
    },
      
    init: function(subscribecb, receivecb, callback) {
      instance.send_receive.receivecb = receivecb;
      instance.send_receive.subscribecb = subscribecb;

      function do_init_partitions(partitions, callback) {
        var keys = Object.keys(partitions);
        var tasks = [];        
        for(var i = 0; i < keys.length; i++) {
          var key = keys[i];

          instance.runtime.partitions[key] = eventhub_partition.Instance(instance.eventhub,
                                                                         instance.group,
                                                                         key,
                                                                         instance.send_receive.messenger);
        }
        instance.runtime.initialized = true;
      }
      
      instance.eventhub.get_group_partitions(instance.group, function(err, partitions) {
        if(err) {
          callback(err);
        } else {
          if(instance.storage.instance) {
            // do we need to load the state?
            var keys = Object.keys(partitions);
            var loaded = true;
            for(var i = 0; i < keys.length; i++) {
              if(!("state" in partitions[keys[i]])) {
                loaded = false;
                break;
              }
            }
            if(!loaded) {
              instance.storage.instance.retrieve_eventhub_state(instance.eventhub.config.space,
                                                                instance.eventhub.config.name,
                                                                instance.group,
                                                                instance.eventhub.runtime.partitions[instance.group],
                                                                function(result) {
                if(result) {
                  for(var i = 0; i < result.length; i++) {
                    var id = result[i].id;
                    if(id in instance.eventhub.runtime.partitions[instance.group]) {
                      instance.eventhub.set_partition_state(instance.group, id, result[i].state);
                    }
                  }
                  do_init_partitions(partitions, callback)
                } else {
                  callback("unable to load partition state");
                }
              });
            } else {
              do_init_partitions(partitions);
              callback(null);
            }
          } else {
            do_init_partitions(partitions);
            callback(null);
          }
        }
      });
    },
    
    receive: function(callback) {
      function do_receive() {      
        instance.send_receive.messenger.on('message', function(message, subscription) {
          var partitionId = subscription.substring(subscription.lastIndexOf("/") + 1);
          if(message.annotations && (message.annotations.length == 2)) {
            var annotationMap = message.annotations[1];
            for(var i = 0; i < annotationMap.length / 2; i++) {
              if(annotationMap[2 * i][1] == 'x-opt-offset') {
                instance.eventhub.update_partition_state(instance.group, partitionId, 'x-opt-offset', annotationMap[2 * i + 1][1]);
                break;
              }
            }
          }
          eventhubutils.parse_message_from_qpid(message);
          if(instance.send_receive.receivecb) {
            instance.send_receive.receivecb(null, { message: message, subscription: subscription });
          }
        });
      
        instance.send_receive.messenger.receive();
      }

      if(!instance.runtime.initialized) {
        callback("event processor not initialized");
      } else {
        if(callback) {
          instance.send_receive.receivecb = callback;
        }

        if(!instance.runtime.subscribed) {
          instance.subscribe(function(err) {
            if(!err) {
              do_receive();
            } else {
              callback(err);
            }
          })
        } else {
          do_receive();
        }
      }
    },
    
    send: function(body, key, callback) {
      if(!instance.runtime.initialized) {
        callback("event processor not initialized");
      } else {  
        var keys = Object.keys(instance.runtime.partitions);
        var partition = instance.runtime.partitions[keys[sendKey++]];
        sendKey = sendKey % keys.length;
        var message = body;
        
        // TODO -- commented out due to an issue between node-qpid and Event Hub where
        // messages timeout.
        if(typeof body === 'object') {
          message = JSON.stringify( { content: body, encoding: "json" } );
        }
        /*
        instance.send_receive.messenger.send( { address: instance.send_receive.senduri,
                                                body: message },
                                                function(err) {
          if(callback) {
            callback(err);
          }
        });
        */
        partition.send(body, key, callback);
      }
    }
  }
        
  return instance;
}

module.exports.Instance = EventProcessorInstance;