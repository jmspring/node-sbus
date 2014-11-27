var qpid        = require('qpid'),
    crypto      = require('crypto'),
    utf8        = require('utf8'),
    xml2js      = require('xml2js'),
    request     = require('request');

function createSharedAccessToken(namespace, hubName, saName, saKey) {
  if (!namespace || !hubName || !saName || !saKey) {
    throw 'Missing required parameter';
  }

  var uri = 'https://' + namespace + '.servicebus.windows.net/' + hubName + '/';

  var encoded = encodeURIComponent(uri);

  var epoch = new Date(1970, 1, 1, 0, 0, 0, 0);
  var now = new Date();
  var year = 365 * 24 * 60 * 60;
  var ttl = ((now.getTime() - epoch.getTime()) / 1000) + (year * 5);

  var signature = encoded + '\n' + ttl;
  var signatureUTF8 = utf8.encode(signature);
  var hash = crypto.createHmac('sha256', saKey).update(signatureUTF8).digest('base64');

  return 'SharedAccessSignature sr=' + encoded + '&sig=' + 
          encodeURIComponent(hash) + '&se=' + ttl + '&skn=' + saName;
}

function parseEventHubInfo(feed, callback) {
  var parser = new xml2js.Parser();
  parser.on('end', function(result) {
    var partitions = [];
    for(var i = 0; i < result["feed"]["entry"].length; i++) {
      var entry = result["feed"]["entry"][i];
      var partition = {
        id:             i,
        published:      entry["published"][0],
        updated:        entry["updated"][0],
        size:           entry["content"][0]["PartitionDescription"][0]["SizeInBytes"][0],
        beginingseq:    entry["content"][0]["PartitionDescription"][0]["BeginSequenceNumber"][0],
        endingseq:      entry["content"][0]["PartitionDescription"][0]["EndSequenceNumber"][0]
      };
      partitions.push(partition);
    }
    callback(partitions);
  });
  parser.parseString(feed);
}

module.exports.create_instance = function(ns, name, user, pass) {
  var username =    user;
  var password =    pass;
  var ehnamespace = ns;
  var ehname =      name;
  var token =       createSharedAccessToken(ehnamespace, ehname, username, password);
  var basehttpuri = 'https://' + ehnamespace + '.servicebus.windows.net/' + ehname + '/';
  var baseamqpuri = 'amqps://' + encodeURIComponent(username) + ':' + encodeURIComponent(password) +
                      '@' + ehnamespace + '.servicebus.windows.net/' + ehname + '/';
  var partitions =  null;

  var instance = {    
    info:   function(callback) {
      request({
        'uri':      basehttpuri + 'ConsumerGroups/$Default/Partitions',
        'method':   'GET',
        'headers':  {
          'Authorization':  token
        }
      }, function(err, response) {
        if(!err && response.statusCode == 200) {
          parseEventHubInfo(response.body, function(result) {
            if(partitions == null) {
              partitions = result;
            }
            callback(null, result);
          });
        } else {
          var code = null;
          if(response && response.hasOwnProperty('statusCode')) {
            code = response.statusCode;
          }
          callback(err || 'Error', code);
        }
      });
    },
    
    event_processor_instance:   function(group, message_cb, subscribe_cb) {
      var consumerbaseuri = baseamqpuri + "ConsumerGroups/"+ group + "/Partitions/";
      var messenger = new qpid.proton.Messenger();
      var message_callback = message_cb || null;
      var subscribe_callback = subscribe_cb || null;
      
      messenger.on('message', function(message, subscription) {
        if(message_callback) {
          message_callback(message, subscription);
        }
      });
      
      messenger.on('subscribed', function(url) {
        if(subscribe_callback) {
          subscribe_callback(url);
        }
      });

      function process_events() {
        // subscribe to each partition
        for(var i = 0; i < partitions.length; i++) {
          var partitionuri = consumerbaseuri + i;
          messenger.subscribe(partitionuri, { }, subscribe_callback);
        }

        // receiving        
        messenger.receive();
      }
      
      var processor_instance = {
        process:  function() {
          // we need to get the partition information
          if(partitions == null) {
            instance.info(function() {
              process_events();
            });
          } else {
            process_events();
          }
        },
        
        subscribe_handler:  function(cb) {
          subscribe_callback = cb;
        },
      
        message_handler:    function(cb) {
          message_callback = cb;
        }
      }
      
      return processor_instance;
    }
  };
  
  return instance;
};
