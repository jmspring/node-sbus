var azure       = require('azure-storage'),
    uuid        = require('node-uuid'),
    crypto      = require('crypto'),
    async       = require('async');

module.exports.azure_store = function(name, key) {
  var accountUrl = name + ".table.core.windows.net";
  var accountName = name;
  var accountKey = key;
  
  function sha1_hash(val) {
    var shasum = crypto.createHash('sha1');
    shasum.update(val);
    return shasum.digest('hex');
  }  
  
  function tablename(ehnamespace, ehname) {
    return "tbl" + sha1_hash(ehnamespace + ":" + ehname);
  }
  
  function partitionkey(consumergroup) {
    return "pk" + sha1_hash(consumergroup);
  }
  
  function rowkey(partition) {
    return "rk" + sha1_hash("partition:" + partition);
  }

  function retrieve_partition_state(tablesvc, ehnamespace, ehname, consumergroup, partitionid, callback) {
    tablesvc.retrieveEntity(tablename(ehnamespace, ehname), partitionkey(consumergroup), rowkey(partitionid), function(error, result, response) {
     callback(error, result, response);
    });
  }
  
  var store = {
    store_eventhub_state: function(ehnamespace, ehname, consumergroup, partitioninfo, callback) {
      var tablesvc = azure.createTableService(accountName, accountKey);
      if(tablesvc) {
        var batch = new azure.TableBatch();
        var entityGen = azure.TableUtilities.entityGenerator;
        var keys = Object.keys(partitioninfo);
        for(var i = 0; i < keys.length; i++) {
          var entity = {
            PartitionKey:   entityGen.String(partitionkey(consumergroup)),
            RowKey:         entityGen.String(rowkey(partitioninfo[keys[i]].id)),
            State:          entityGen.String(JSON.stringify(partitioninfo[keys[i]].state))
          };
          batch.insertOrReplaceEntity(entity);
        }
        tablesvc.executeBatch(tablename(ehnamespace, ehname), batch, function(error, result, response) {
          callback(error, result, response);
        });
      } else {
        // TODO -- error out
      }
    },
    
    retrieve_eventhub_state: function(ehnamespace, ehname, consumergroup, partitioninfo, callback) {
      var tablesvc = azure.createTableService(accountName, accountKey);
      if(tablesvc) {
        var asyncTasks = [];
        var partitionResults = [];
        var paritionKeys = Object.keys(partitioninfo);
        paritionKeys.forEach(function(key) {
          var partition = partitioninfo[key];
          var partitionid = partition.id;
          asyncTasks.push(function(cb) {
            retrieve_partition_state(tablesvc, ehnamespace, ehname, consumergroup, partitionid, function(error, result, response) {
              var partitionState = { id: partitionid };
              if(!error) {
                partitionState["state"] = JSON.parse(result.State["_"]);
              } else {
                if(error.statusCode && error.statusCode == 404) {
                  partitionState["state"] = {};
                } else {
                  partitionState["error"] = error;
                }
              }
              cb(null, partitionState);
            });
          });
        });
        async.parallel(asyncTasks, function(err, results) {
          callback(results);
        });
      }
    }
  };
  
  return store;
}