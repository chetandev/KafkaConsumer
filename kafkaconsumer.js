
//HGH LEVEL CONSUMER 
var consumerId = Date.now();
console.log('consumer Id:'+consumerId);
var Promise = require('bluebird');
var cassBl = require('./cass.js');
var kafka = require('kafka-node'),
    HighLevelConsumer = kafka.HighLevelConsumer,
    client = new kafka.Client('172.24.36:2181,172.24.1.189:2181'),
    consumer = new HighLevelConsumer(
        client, [
            { topic: 'textmessages' }
        ],
        {
            groupId: 'nodeconsumer',
            id:consumerId
        }
    );




consumer.on('message', function(message) {
    console.log('msg arrived ' + message.value);
    //put data in cassandra 
    cassBl.put_in_cass(JSON.parse(message.value).messages)
        .then(function(result) {

            console.log('ConsumerId: '+consumerId+'   result: '+result);
        })
        .catch(function(err) {
            console.log('error occured while writing in cassandra:' + err)
        })
});



consumer.on('error', function(err) {
    console.log('error occured in kafka consumer ' + err);
});





