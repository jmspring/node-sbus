var should      = require('should'),

    EHPartition = require('../lib/eventhub_partition');

function EHMock(group, partition, offset, amqpMock) {
    var mock = {
        amqpMock: amqpMock,
        group: group,
        partition: partition,
        partition_send_uri: 'amqps://sbns/eh/Partitions/' + partition,
        partition_receive_uri: 'amqps://sbns/eh/ConsumerGroups/' + group + '/Partitions/' + partition,
        config: {
            name: 'eh',
            space: 'sbns',
            baseamqpuri: 'amqps://sbns/eh/'
        },
        runtime: {
            partitions: {}
        }
    };
    mock.runtime.partitions[group] = {};
    mock.runtime.partitions[group][partition] = {
        state: {
            'x-opt-offset': offset
        }
    };
    mock.get_partition_state = function(group, id) {
        return this.runtime.partitions[group][id].state;
    };
    mock.set_partition_state = function(group, id, state) {
        this.runtime.partitions[group][id].state = state;
    };
    mock.update_partition_state = function(group, id, key, value) {
        this.runtime.partitions[group][id].state[key] = value;
    };
    mock.get_partition = function() {
        return EHPartition.Instance(this, this.group, this.partition, this.amqpMock);
    };
    return mock;
}

function AMQPMock() {
    this.receivedMessages = [];
    this.registeredReceivers = {};
}

AMQPMock.prototype.eventHubSend = function(uri, payload, partitionKey, cb) {
    this.receivedMessages.push({ uri: uri, payload: payload, partitionKey: partitionKey });
    cb(null);
};

AMQPMock.prototype.eventHubReceive = function(uri, offset, cb) {
    this.registeredReceivers[uri] = { offset: offset, callback: cb };
};

AMQPMock.prototype._fromServer = function(uri, partitionId, payload, offset) {
    if (this.registeredReceivers[uri]) {
        if (!this.registeredReceivers[uri].offset || this.registeredReceivers[uri].offset <= offset) {
            this.registeredReceivers[uri].callback(null, partitionId, payload, { 'x-opt-offset': offset });
        }
    }
};

describe('EventHubPartition', function() {
    describe('#send()', function (done) {
        it('should send to amqp client', function (done) {
            var ehMock = EHMock('$Default', '1', '0', new AMQPMock());
            var partition = ehMock.get_partition();
            partition.send({ test: 'message' }, function() {
                var msgs = ehMock.amqpMock.receivedMessages;
                msgs.length.should.eql(1);
                msgs[0].should.eql({
                    uri: ehMock.partition_send_uri,
                    payload: { test: 'message'},
                    partitionKey: null
                });
                done();
            });
        });
    });

    describe('#receive()', function() {
        it('should receive sent messages', function(done) {
            var ehMock = EHMock('$Default', '1', '0', new AMQPMock());
            var partition = ehMock.get_partition();
            partition.receive(function (err, id, payload) {
                id.should.eql('1');
                payload.should.eql({ test: 'message' });
                done();
            });
            ehMock.amqpMock._fromServer(ehMock.partition_receive_uri, '1', { test: 'message' }, '5');
        });

        it('should update partition state on receipt', function(done) {
            var ehMock = EHMock('$Default', '1', '0', new AMQPMock());
            var partition = ehMock.get_partition();
            partition.receive(function (err, id, payload) {
                var state = ehMock.get_partition_state('$Default', '1');
                state['x-opt-offset'].should.eql('5');
                done();
            });
            ehMock.amqpMock._fromServer(ehMock.partition_receive_uri, '1', { test: 'message' }, '5');
        });
    });
});