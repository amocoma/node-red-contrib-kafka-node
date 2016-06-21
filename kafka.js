module.exports = function(RED) {

    function kafkaProducerNode(config) {
        RED.nodes.createNode(this,config);
        var node = this;
        var Kafka = require('no-kafka');
        // Retrieve the config node
        this.server = RED.nodes.getNode(config.server);
        
        var gCtx = node.context().global;
        node.log(JSON.stringify(gCtx.get('process').env));
        if (this.server) {
            var clusterZookeeper = this.server.zkquorum,
                topics = String(config.topics), // not used right now!
                sslOptions = {key:kafkaConfig.clientCertKey, cert:kafkaConfig.clientCert, ca:kafkaConfig.trustedCert},
                zkOptions = {connectionString:'kafka+ssl://ec2-52-51-56-194.eu-west-1.compute.amazonaws.com:9096,kafka+ssl://ec2-52-50-127-83.eu-west-1.compute.amazonaws.com:9096', ssl: sslOptions},
                producer = new Kafka.Producer(zkOptions);
            try {
                this.on("input", function(msg) {
                    console.log('XXXXX : ' + 'input');
                    return producer.init().then(function(){
                      console.log('XXXXX : ' + 'PRODUCER INITIALIZED');
                      return producer.send({
                          topic: 'test',
                          partition: 0,
                          message: {
                              value: 'Hello!'
                          }
                      });
                    })
                    .then(function (result) {
                        console.log('XXXXX : ' + 'NO INIT OF PRODUCER');
                      node.log(' xxxxxx result : ' + JSON.stringify(result));
                    });
                });
            }catch(e) {
                node.error(e);
            }
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
            /**
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
            **/
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
