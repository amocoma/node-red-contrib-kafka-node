/**
 * Created by fwang1 on 3/25/15.
 */
module.exports = function(RED) {
    /*
     *   Kafka Producer
     *   Parameters:
     - topics 
     - zkquorum(example: zkquorum = “[host]:2181")
     */
    function kafkaNode(config) {
        console.log('here you go');
        RED.nodes.createNode(this,config);
        var topic = config.topic;
        var clusterZookeeper = config.zkquorum;
        var node = this;
        var kafka = require('kafka-node');
        var HighLevelProducer = kafka.HighLevelProducer;
        var Client = kafka.Client;
        var topics = config.topics;
        var client = new Client(clusterZookeeper);

        try {
            this.on("input", function(msg) {
                var payloads = [];

                // check if multiple topics
                if (topics.indexOf(",") > -1){
                    var topicArry = topics.split(',');

                    for (i = 0; i < topicArry.length; i++) {
                        payloads.push({topic: topicArry[i], messages: msg.payload});
                    }
                }
                else {
                    payloads = [{topic: topics, messages: msg.payload}];
                }

                producer.send(payloads, function(err, data){
                    if (err){
                        node.error(err);
                    }
                    node.log("Message Sent: " + data);
                });
            });
        }
        catch(e) {
            node.error(e);
        }
        var producer = new HighLevelProducer(client);
    }

    RED.nodes.registerType("kafka",kafkaNode);


    function kafkaSubscriberNode(config) {
        RED.nodes.createNode(this,config);
        var node = this;
        // Retrieve the config node
        this.server = RED.nodes.getNode(config.server);

        if (this.server) {
            var hlConsumer = kafka.HighLevelConsumer,
                topics = String(config.topics),
                clusterZookeeper = server.zkquorum,
                zkOptions = {key:server.key, cert:server.cert, ca:server.ca},
                client = new (require('kafka-node')).Client(clusterZookeeper, zkOptions),
                kafkaOptions = {
                    groupId: config.groupId,
                    autoCommit: config.autoCommit,
                    autoCommitMsgCount: 10,
                };
                topicJSONArry = [];
            if(topics!=null && topics.trim().length > 0){
                if (topics.indexOf(",") != -1){
                    topics.split(',').forEach(function(_topic){topicJSONArry.push({topic: _topic.trim()});});
                    topics = topicJSONArry;
                }else{
                    topics = [{topic:topics.trim()}];
                }
                try {
                    var consumer = new hlConsumer(client, topics, options);
                    this.log("Consumer created...");

                    consumer.on('message', function (message) {
                        console.log(message);
                        node.log(message);
                        var msg = {payload: message};
                        node.send(msg);
                    });            
                    consumer.on('error', function (err) {
                       console.error(err);
                    });
                }catch(e){
                    node.error(e);
                    return;
                } 
            }else{
                console.error('No topics configures');
            }

        }else{
            node.log('No config node configured');
            // 
        }
    }


    RED.nodes.registerType("kafka-sub",kafkaSubscriberNode);

    function kafkaServerConfigNode(n) {
        RED.nodes.createNode(this,n);
        this.name = n.name;
        this.zkquorum = n.zkquorum;
        this.key = n.key;
        this.cert = n.cert;
        this.ca = n.ca;
    }
    RED.nodes.registerType("kafka-server",kafkaServerConfigNode);

};
