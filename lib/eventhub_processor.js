var debug       = require('debug')('sbus-eventhub.processor'),
    async       = require('async'),
    request     = require('request'),
    storage     = require('./storage'),
    eventhub    = require('./eventhub'),
    eventhubutils = require('./eventhubutils');
    
function EventProcessorInstance(eventhub, group, amqpProvider) {
  if(group === null) {
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
      amqpProvider:       amqpProvider,
      senduri:            eventhub.config.baseamqpuri,
      receivecb:          null
    },
    
    runtime: {
      initialized:        false,
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
        if(instance.storage.updater !== null) {
          clearInterval(instance.storage.updater);
          instance.storage.updater = null;
        }
      }
    },
    
    update_hub_state: function(callback) {
      if(instance.storage.instance) {
        var partitions = {};
        Object.keys(instance.runtime.partitions).forEach(function(id) {
          var state = instance.eventhub.get_partition_state(instance.group, id);
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
      } else {
        if (callback) {
          callback(null);
        }
      }
    },
    
    init: function(receivecb, callback) {
      instance.send_receive.receivecb = receivecb;

      function do_init_partitions(partitions, callback) {
        var keys = Object.keys(partitions);
        var tasks = [];        
        for(var i = 0; i < keys.length; i++) {
          var key = keys[i];

          debug('Instantiating partition ' + key + ' for ' + instance.eventhub.config.name + ', group ' + instance.group);
          instance.runtime.partitions[key] = eventhub_partition.Instance(instance.eventhub,
                                                                         instance.group,
                                                                         key,
                                                                         instance.send_receive.amqpProvider);
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
                  do_init_partitions(partitions, callback);
                  callback(null);
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

    teardown: function(callback) {
      if (instance.runtime.initialized) {
        var self = this;
        this.update_hub_state(function() {
          self.send_receive.amqpProvider.disconnect(function () {
            self.runtime.initialized = false;
            if (callback) {
              callback();
            }
          });
        });
      }
    },
    
    receive: function(callback) {
      if(!instance.runtime.initialized) {
        callback("event processor not initialized");
      } else {
        if(callback) {
          instance.send_receive.receivecb = callback;
        } else {
          callback = instance.send_receive.receivecb;
        }

        for (var idx in instance.runtime.partitions) {
          var curPartition = instance.runtime.partitions[idx];
          debug('Starting receipt for ' + curPartition.id);
          curPartition.receive(callback);
        }
      }
    },
    
    send: function(body, key, callback) {
      if(!instance.runtime.initialized) {
        callback("event processor not initialized");
      } else {
        if(key) {
          debug('Sending message to ' + key + ': ');
          debug(body);
          instance.send_receive.amqpProvider.eventHubSend(instance.send_receive.senduri, body, key, callback);
        } else {
          var keys = Object.keys(instance.runtime.partitions);
          var partition = instance.runtime.partitions[keys[sendKey++]];
          sendKey = sendKey % keys.length;
          debug('Sending message to random partition ' + partition.id + ': ');
          debug(body);
          partition.send(body, callback);
        }
      }
    }
  };
        
  return instance;
}

module.exports.Instance = EventProcessorInstance;