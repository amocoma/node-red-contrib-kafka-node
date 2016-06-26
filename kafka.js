module.exports = function(RED) {

    function kafkaProducerNode(config) {
        RED.nodes.createNode(this,config);
        var node = this;
        var Kafka = require('no-kafka');
        // Retrieve the config node
        this.server = RED.nodes.getNode(config.server);
        var env = node.context().global.get('process').env;
        if (this.server) {
            var clusterZookeeper = this.server.zkquorum,
                topics = String(config.topics), // not used right now!
                sslOptions = {key:env.KAFKA_CLIENT_CERT_KEY, cert:env.KAFKA_CLIENT_CERT, ca:env.KAFKA_TRUSTED_CERT, checkServerIdentity: function (host, cert) {return undefined;}},
                zkOptions = {connectionString:env.KAFKA_URL, ssl: sslOptions},
                producer = new Kafka.Producer(zkOptions);
            try {
                this.on("input", function(msg) {
                    return producer.init().then(function(){
                      console.log('XXXXX : ' + 'PRODUCER INITIALIZED');
                      return producer.send({
                          topic: 'test',
                          partition: 0,
                          message: {
                              value: 'Hello!'
                          }
                      });
                    }).then(function (result) {
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
        var node = this,
            Promise = require('bluebird'),
            Kafka = require('no-kafka'),
            env = node.context().global.get('process').env;
        // Retrieve the config node
        this.server = RED.nodes.getNode(config.server);
        if (this.server) {
            var clusterZookeeper = this.server.zkquorum,
                sslOptions = {key:env.KAFKA_CLIENT_CERT_KEY, cert:env.KAFKA_CLIENT_CERT, ca:env.KAFKA_TRUSTED_CERT, checkServerIdentity: function (host, cert) {return undefined;}},
                zkOptions = {connectionString: env.KAFKA_URL, ssl: sslOptions},
                consumer = new Kafka.GroupConsumer(zkOptions);
            var dataHandler = function (messageSet, topic, partition) {
                return Promise.each(messageSet, function (m){
                            console.log(topic, partition, m.offset, m.message);
                            node.send({payload: m.message});
                            // commit offset
                            return consumer.commitOffset({topic: topic, partition: partition, offset: m.offset, metadata: 'optional'});
                        });
            };

            var strategies = [{
                strategy: 'TestStrategy',
                subscriptions: ['test'],
                handler: dataHandler
            }];

            consumer.init(strategies); // all done, now wait for messages in dataHandler

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
