var debug       = require('debug')('sbus-eventhub.partition'),
    request     = require('request'),
    storage     = require('./storage'),
    eventhub    = require('./eventhub'),
    eventhubutils = require('./eventhubutils');

function PartitionInstance(eventhub, group, id, amqpProvider)
{
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
      amqpProvider:   amqpProvider,
      subscribeuri:   eventhub.config.baseamqpuri + "ConsumerGroups/" + group + "/Partitions/" + id,
      messageuri:     eventhub.config.baseamqpuri + "Partitions/" + id,
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
   
    receive: function(callback) {
      // if a callback is specified, we register it locally
      if(callback) {
        instance.send_receive.receivecb = callback;
      }

      var partitionInfo = instance.eventhub.runtime.partitions[instance.group][instance.id];

      function do_receive() {
        var offset;
        if(("state" in partitionInfo) && ("x-opt-offset" in partitionInfo.state)) {
          offset = partitionInfo.state['x-opt-offset'];
        }
        debug('Start Receiving for ' + instance.send_receive.subscribeuri + ' at offset ' + (offset || '0'));
        instance.send_receive.amqpProvider.eventHubReceive(instance.send_receive.subscribeuri, offset, function (err, partitionId, payload, annotations) {
          if (partitionId === instance.id) {
            if (err) {
              callback(err);
            } else {
              debug('Received message for partition ' + partitionId + ': ');
              debug(payload);
              debug('Annotations: ');
              debug(annotations);
              instance.eventhub.update_partition_state(instance.group, instance.id, 'x-opt-offset', annotations['x-opt-offset']);
              callback(null, instance.id, payload);
            }
          }
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
                  do_receive();
                }
              } else {
                callback("unable to retrieve partition info");
              }
            });
        } else {
          do_receive();
        }
    },
    
    send: function(body, callback) {
      instance.send_receive.amqpProvider.eventHubSend(instance.send_receive.messageuri, body, null, callback);
    }
  };
  
  return instance;
}

module.exports.Instance = PartitionInstance;
