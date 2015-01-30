var debug       = require('debug')('sbus-eventhub'),
    request     = require('request'),
    storage     = require('./storage'),
    eventhubutils = require('./eventhubutils');
    eventhub_partition = require('./eventhub_partition');
    eventhub_processor = require('./eventhub_processor');

function EventHubInstance(ns, name, user, pass, amqpProvider) {
  var instance = {
    // event hub config information
    
    config:   {
      space:          ns,
      name:           name,
      user:           user,
      password:       pass,
      token:          eventhubutils.createSharedAccessToken(ns, name, user, pass),
      basehttpuri:    'https://' + ns + '.servicebus.windows.net/' + name + '/',
      baseamqpuri:    'amqps://' + encodeURIComponent(user) + ':' + encodeURIComponent(pass) +
                        '@' + ns + '.servicebus.windows.net/' + name + '/'
    },
    
    runtime:    {
      amqpProvider:   amqpProvider,
      partitions:     {}          // partition information indexed by consumer group then partition id
    },
    
    info: function(group, callback) {
      if(group === null) {
        group = "$Default";
      }
      request({
        'uri':      instance.config.basehttpuri + 'ConsumerGroups/' + group + '/Partitions',
        'method':   'GET',
        'headers':  {
          'Authorization':  instance.config.token
        }
      }, function(err, response) {
        if(!err && response.statusCode == 200) {
          eventhubutils.parseEventHubInfo(response.body, function(result) {
            instance.runtime.partitions[group] = result;
            callback(null, result);
          });
        } else {
        
          var code = null;
          if(response && response.hasOwnProperty('statusCode')) {
            code = response.statusCode;
          }
          callback(err || 'error', code);
        }
      });
    },
    
    get_group_partitions: function(group, callback) {
      if(!(group in instance.runtime.partitions)) {
        instance.info(group, function(err, result) {
          if(err) {
            callback(err);
          } else {
            callback(null, result);
          }
        });
      } else {
        callback(null, instance.runtime.partitions[group]);
      }
    },
    
    get_partition_state: function(group, id) {
      var state = null;
      if(group in instance.runtime.partitions) {
        if(id in instance.runtime.partitions[group]) {
          if("state" in instance.runtime.partitions[group][id]) {
            state = instance.runtime.partitions[group][id].state;
            debug('Partition state for '+id+' in '+group+': ');
            debug(state);
          }
        }
      }
      return state;
    },
    
    set_partition_state: function(group, id, state) {
      if(group in instance.runtime.partitions) {
        if(id in instance.runtime.partitions[group]) {
          instance.runtime.partitions[group][id].state = state;
        } else {
          // TODO -- unknown id error
        }
      } else {
        // TODO -- unknown group error
      }   
    },
    
    update_partition_state: function(group, id, key, value) {
      if(!(group in instance.runtime.partitions) || !(id in instance.runtime.partitions[group])) {
        // TODO -- err somehow?
        return;
      }
      if(!("state" in instance.runtime.partitions[group][id])) {
         instance.runtime.partitions[group][id].state = {};
      }
      instance.runtime.partitions[group][id].state[key] = value;
    },
    
    getPartition: function(id, group, callback) {
      if(group === null) {
        group = "$Default";
      }

      function get_partition(group, id, messenger, subscribed) {
        return eventhub_partition.Instance(instance, group, id, instance.runtime.amqpProvider);
      }

      if(!(group in instance.runtime.partitions)) {
        instance.info(group, function(err, result) {
          if(err) {
            callback(err);
          } else {
            var partitionInfo = null;
            if(id in result) {
              partitionInfo = result[id];
            }
            
            if(partitionInfo) {
              callback(null, get_partition(group, id, null, false));
            } else {
              callback("unable to get partition info");
            }
          }
        });
      } else {
        callback(null, get_partition(group, id, null, false));
      }
    },
    
    getRandomPartition: function(group, callback) {
      function randomPartition(group) {
        var keys = Object.keys(instance.runtime.partitions[group]);
        var i = Math.floor(Math.random() * keys.length);
        return eventhub_partition.Instance(instance, group, keys[i], null, false);
      }
    
      if(group === null) {
        group = "$Default";
      }
  
      if(Object.keys(instance.runtime.partitions).length === 0) {
        instance.info(group, function(err, result) {
          if(err) {
            callback(err);
          } else {
            callback(null, randomPartition(group));
          }
        });
      } else {
        callback(null, randomPartition(group));
      }
    },
    
    getEventProcessor: function(group, callback) {
      if(group === null) {
        group = "$Default";
      }
      
      function get_event_processor(group) {
        return eventhub_processor.Instance(instance, group, instance.runtime.amqpProvider);
      }

      if(!(group in instance.runtime.partitions)) {
        instance.info(group, function(err, result) {
          if(err) {
            callback(err);
          } else {
            callback(null, get_event_processor(group));
            
          }
        });
      } else {
        callback(null, get_event_processor(group));
      }
    }
  };
  
  return instance;
}

module.exports.EventHub = {
  Instance: EventHubInstance
};