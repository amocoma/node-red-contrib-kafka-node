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
    function kafkaProducerNode(config) {
        RED.nodes.createNode(this,config);
        var node = this;
        // Retrieve the config node
        this.server = RED.nodes.getNode(config.server);
        node.log(JSON.stringify(this.server));
        if (this.server) {
            var kafka = require('kafka-node'),
                hlProducer = kafka.HighLevelProducer,
                clusterZookeeper = this.server.zkquorum,
                topics = String(config.topics),
                zkOptions = {key:this.server.key, cert:this.server.cert, ca:this.server.ca},
                client = new kafka.Client(clusterZookeeper, zkOptions);
            try {
                this.on("input", function(msg) {
                    var payloads = [];
                    topics.split(',').forEach(function(_topic){payloads.push({topic: _topic.trim(), messages: msg.payload});});
                    producer.send(payloads, function(err, data){if(err)node.error(err); else node.log("Message Sent: " + data);});
                });
            }catch(e) {
                node.error(e);
            }
            var producer = new hlProducer(client);
        }else{
            node.log('No config node configured');
        }
    }

    RED.nodes.registerType("kafka-prod",kafkaProducerNode);

    function kafkaSubscriberNode(config) {
        RED.nodes.createNode(this,config);
        var node = this;
        // Retrieve the config node
        this.server = RED.nodes.getNode(config.server);

        if (this.server) {
            var hlConsumer = kafka.HighLevelConsumer,
                topics = String(config.topics),
                clusterZookeeper = this.server.zkquorum,
                zkOptions = {key:this.server.key, cert:this.server.cert, ca:this.server.ca},
                client = new (require('kafka-node')).Client(clusterZookeeper, zkOptions),
                kafkaOptions = {
                    groupId: config.groupId,
                    autoCommit: config.autoCommit,
                    autoCommitMsgCount: 10,
                },
                topicJSONArry = [];
            if(topics!=null && topics.trim().length > 0){
                    topics.split(',').forEach(function(_topic){topicJSONArry.push({topic: _topic.trim()});});
                try {
                    var consumer = new hlConsumer(client, topicJSONArry, kafkaOptions);


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
