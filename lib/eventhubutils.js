var crypto      = require('crypto'),
    utf8        = require('utf8'),
    xml2js      = require('xml2js'); 
    
// default interval for when state is stored in milliseconds
module.exports.DEFAULT_STORAGE_INTERVAL = 15000;   
    
module.exports.createSharedAccessToken = function(namespace, hubName, saName, saKey) {
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

module.exports.parseEventHubInfo = function(feed, callback) {
  var parser = new xml2js.Parser();
  parser.on('end', function(result) {
    var partitions = {};
    for(var i = 0; i < result["feed"]["entry"].length; i++) {
      var entry = result["feed"]["entry"][i];
      var partition = {
        id:             entry["title"][0]["_"],
        published:      entry["published"][0],
        updated:        entry["updated"][0],
        size:           entry["content"][0]["PartitionDescription"][0]["SizeInBytes"][0],
        beginingseq:    entry["content"][0]["PartitionDescription"][0]["BeginSequenceNumber"][0],
        endingseq:      entry["content"][0]["PartitionDescription"][0]["EndSequenceNumber"][0]
      };
      partitions[partition["id"]] = partition;
    }
    callback(partitions);
  });
  parser.parseString(feed);
}

// takes a message generated from node-qpid and returns a massaged/cleaned up message
module.exports.parse_message_from_qpid = function(message) {
  // TODO -- FIX THIS HACK.  node-qpid returns everything as a string currently
  // and that string has added quotes.  This removes those quotes.
  message.body = message.body.substr(1, message.body.length - 2);

  // TODO -- FIX THIS HACK.  node-qpid does not support passing objects only
  // strings.  In order to fix this, send serializes the object to JSON.  This
  // hack deserializes objects.
  if((message.body.indexOf("\"content\":") > -1) && (message.body.indexOf("\"encoding\":\"json\"") > -1)) {
    message.body = JSON.parse(message.body);
  }  
}