module.exports = function (RED) {
  const { Kafka } = require('kafkajs');
  const { v4: uuidv4 } = require('uuid');

  function KafkajsConsumerNode(config) {
    const node = this;
    RED.nodes.createNode(this, config);

    const client = RED.nodes.getNode(config.client);

    if (!client) {
      return;
    }

    // buffer to store received messages
    let messageBuffer = [];
    const unackMessages = new Set();
    let paused = false; // flag to determine if the consumer is paused

    const kafka = new Kafka(client.options)

    const consumerOptions = new Object();
    consumerOptions.groupId = config.groupid ? config.groupid : "kafka_js_" + uuidv4();

    const subscribeOptions = new Object();
    subscribeOptions.topic = config.topic;

    const runOptions = new Object();

    if (config.advancedoptions) {
      consumerOptions.sessionTimeout = config.sessiontimeout;
      consumerOptions.rebalanceTimeout = config.rebalancetimeout;
      consumerOptions.heartbeatInterval = config.heartbeatinterval;
      consumerOptions.metadataMaxAge = config.metadatamaxage;
      consumerOptions.allowAutoTopicCreation = config.allowautotopiccreation;
      consumerOptions.maxBytesPerPartition = config.maxbytesperpartition;
      consumerOptions.minBytes = config.minbytes;
      consumerOptions.maxBytes = config.maxbytes;
      consumerOptions.maxWaitTimeInMs = config.maxwaittimeinms;

      subscribeOptions.fromBeginning = config.frombeginning;

      runOptions.autoCommitInterval = config.autocommitinterval;
      runOptions.autoCommitThreshold = config.autocommitthreshold;
    }

    node.onError = function (e) {
      node.error("Kafka Consumer Error", e.message);
      node.status({ fill: "red", shape: "ring", text: "Error" });
    }

    node.init = async function init() {
      if (config.advancedoptions && config.clearoffsets) {
        node.status({ fill: "yellow", shape: "ring", text: "Clearing Offset" });
        const admin = kafka.admin();
        await admin.connect();
        await admin.resetOffsets({ groupId: config.groupid, topic: config.topic });
        await admin.disconnect()
      }

      messageBuffer = [];
      unackMessages.clear();
      paused = false;

      node.consumer = kafka.consumer(consumerOptions);
      node.status({ fill: "yellow", shape: "ring", text: "Initializing" });

      node.onConnect = function () {
        node.lastMessageTime = new Date().getTime();
        node.status({ fill: "green", shape: "ring", text: "Ready" });
      }

      node.onDisconnect = function () {
        node.status({ fill: "red", shape: "ring", text: "Offline" });
      }

      node.onRequestTimeout = function () {
        node.status({ fill: "red", shape: "ring", text: "Timeout" });
      }

      // helper function that pauses the reception of messages from the broker
      node.pause = function () {
        node.log(`Pausing execution for topic: ${config.topic}`);
        node.consumer.pause([{ topic: config.topic }]);
        node.status({ fill: 'yellow', shape: 'dot', text: 'Paused' });
        paused = true;
      }

      // helper function that resumes the reception of messages from the broker
      node.resume = function () {
        if (paused) {
          node.consumer.resume([{ topic: config.topic }]);
          node.debug(`Resuming subscription on topic ${config.topic}`);

          node.status({ fill: 'green', shape: 'dot', text: 'Reading' });
          paused = false;
        }
      }

      // function that sends a message to upstream nodes
      node.releaseMessage = function () {
        if (messageBuffer.length > 0) {
          const message = messageBuffer.shift(); // take the first element in the buffer
          node.send(message);
          unackMessages.add(message._msgid);
        }
        if (unackMessages.size < config.maxbuffersize * config.bufferresumethreshold) {
          node.resume();
        }
        node.status({ fill: paused ? 'yellow' : 'green', shape: 'dot', text: `${paused ? 'Paused' : 'Reading'} (Buf:${100 * messageBuffer.length / config.maxbuffersize}%, UnACK:${unackMessages.size})` })
      }

      // adds a message to the send queue and checks if the reception should be paused
      node.queue = function (msg) {
        //console.log(msg)
        messageBuffer.push(msg);
        // console.log(`Message queued ${messageBuffer.length}/${config.maxbuffersize}`)
        if (unackMessages.size >= config.maxbuffersize) {
          node.pause();
        }
      }

      node.onMessage = function (topic, partition, message) {
        node.lastMessageTime = new Date().getTime();
        const payload = new Object();
        payload.topic = topic;
        payload.partition = partition;

        payload.payload = new Object();
        payload.payload = message;

        payload._msgid = uuidv4();

        payload.payload.key = message.key ? message.key.toString() : null;
        payload.payload.value = message.value.toString();

        for (const [key, value] of Object.entries(payload.payload.headers)) {
          payload.payload.headers[key] = value.toString();
        }

        node.queue(payload);
        node.releaseMessage();
      }

      function checkLastMessageTime() {
        if (node.lastMessageTime != null) {
          timeDiff = new Date().getTime() - node.lastMessageTime;
          if (timeDiff > 5000) {
            node.status({ fill: "yellow", shape: "ring", text: "Idle" });
          }
        }
      }

      node.interval = setInterval(checkLastMessageTime, 1000);

      node.consumer.on(node.consumer.events.CONNECT, node.onConnect);
      node.consumer.on(node.consumer.events.DISCONNECT, node.onDisconnect);
      node.consumer.on(node.consumer.events.REQUEST_TIMEOUT, node.onRequestTimeout);

      await node.consumer.connect();
      await node.consumer.subscribe(subscribeOptions);

      // set the eachMessage callback function, executed on each message received from the topic
      runOptions.eachMessage = async ({ topic, partition, message }) => {
        node.onMessage(topic, partition, message);
      }

      await node.consumer.run(runOptions);
    }

    node.init().catch(e => {
      node.error(e)
      node.onError(e);
    });

    node.on('close', function (done) {
      node.consumer.disconnect().then(() => {
        node.status({});
        clearInterval(node.interval);
        done();
      }).catch(e => {
        node.onError(e);
      });
    });

    node.on('input', function (msg) {
      if (msg.tick) {
        if (unackMessages.delete(msg.tick)) {
          node.releaseMessage();
        }
      }
      // This should be used only for debugging purposes
      if (msg.flush) {
        messageBuffer = [];
        unackMessages.clear();
        node.resume();
      }
    })
  }
  RED.nodes.registerType("kafkajs-consumer-throttled", KafkajsConsumerNode);
}
