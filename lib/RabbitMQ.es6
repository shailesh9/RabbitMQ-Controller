"use strict";

import * as amqp from "amqplib";
import {EventEmitter} from "events";

export class RabbitMQ extends EventEmitter {

  constructor(rabbitConfig, loggerInstance) {

    super();

    if (!rabbitConfig || !rabbitConfig.url) {
      throw new Error("Missing RabbitMq Dependencies");
    }

    /** @member {string} config for rabbit mq. */
    this.rabbitConfig_ = rabbitConfig;

    /** @member {string} url to connect with rabbit mq. */
    this.connectionUrl_ = rabbitConfig.url;

    /** @member {string} url to connect with rabbit mq. */
    this.connection_ = null;

    /** @member {string} url to connect with rabbit mq. */
    this.channel_ = null;

    /** @member {string} url to connect with rabbit mq. */
    this.connectionOptions_ = rabbitConfig.options || {};

    this.queueOptions_ = {
      "durable": true,
      "autoDelete": false,
      "arguments": {
        "messageTtl": 24 * 60 * 60 * 1000,
        "maxLength": 1000,
        "deadLetterExchange": `deadLetter${rabbitConfig.queueName}`
      }
    };

    this.exchangeOptions_ = {
      "durable": true,
      "autoDelete": false,
      "alternateExchange": "",
      "arguments": {}
    };

    /** @member {Object} logger instance for logging. */
    this.logger_ = loggerInstance;

  }

  connect() {
    this.connection_ = this.connection_ || amqp.connect(this.connectionUrl_, this.connectionOptions_);
    this.logger_.debug(`${RabbitMQ.name}.connect(), Creating New Connection ===> `);
    return this.connection_;
  }

  createChannel() {

    this.channel_ = this.channel_ ||
      this.connect()
        .then(conn => {
          this.logger_.debug(`${RabbitMQ.name}.createChannel(), Creating New Channel ==> `);
          return conn.createChannel();
        })
        .catch(err => {
          this.logger_.debug("Error in creating RabbitMQ Channel ", err);
        });

    return this.channel_;
  }

  publish(message, queue = null) {

    let {queueName, exchangeName, exchangeType} = this.rabbitConfig_;

    queueName = queue || queueName;

    return this.createChannel()
      .then(channel => {
        this.logger_.debug(`${RabbitMQ.name}.publish(): Publishing the message in queue: ${queueName} `);

        this.logger_.debug(`${RabbitMQ.name}.setChannelPrefetch(): Initializing channel prefetch ==> `);
        channel.prefetch(this.rabbitConfig_.prefetchCount, false);
        this.logger_.debug("Prefetch count: %s successfully set for channel", this.rabbitConfig_.prefetchCount);

        channel.assertExchange(exchangeName, exchangeType, this.exchangeOptions_);

        channel.assertQueue(queueName, this.queueOptions_)
          .then(queueRes => {
            channel.bindQueue(queueRes.queue, exchangeName, queueRes.queue);

            channel.publish(exchangeName, queueRes.queue, new Buffer(message));
            this.logger_.debug(" [x] Sent %s", message);
          });
      })
      .catch(err => {
        this.logger_.debug("Error in publishing the message", err);
        this.connection_ = null;
        this.channel_ = null;
        this.emit("error", err);
      });
  }

  consume(queue = null) {

    let {queueName, exchangeName, exchangeType} = this.rabbitConfig_;

    queueName = queue || queueName;

    return this.createChannel()
      .then(channel => {

        this.logger_.debug(`${RabbitMQ.name}.consume(): Consuming the message from queue: ${queueName} `);

        this.logger_.debug(`${RabbitMQ.name}.setChannelPrefetch(): Initializing channel prefetch ==> `);
        channel.prefetch(this.rabbitConfig_.prefetchCount, false);
        this.logger_.debug("Prefetch count: %s successfully set for channel", this.rabbitConfig_.prefetchCount);

        channel.assertExchange(exchangeName, exchangeType, this.exchangeOptions_);

        channel.assertQueue(queueName, this.queueOptions_)
          .then(queueRes => {

            channel.bindQueue(queueRes.queue, exchangeName, queueRes.queue);

            channel.consume(queueRes.queue, msg => {
              this.logger_.debug("Consuming Message...", msg.content.toString());

              this.emit("msgReceived", msg);

            }, {"noAck": false});

          });
      })
      .catch(err => {
        this.logger_.debug("Error in consuming the message", err);
        this.connection_ = null;
        this.channel_ = null;
        this.emit("error", err);
      });
  }

  acknowledgeMessage(msg, allUpTo = false) {

    return this.createChannel()
      .then(channel => {
        channel.ack(msg, allUpTo);
        this.logger_.debug("Message has been acknowledged... ", msg.content.toString());
      })
      .catch(err => {
        this.logger_.debug("Error in consuming the message", err);
        this.connection_ = null;
        this.channel_ = null;
        this.emit("error", err);
      });

  }

}

// let config = {
//     "url": "amqp://shakti:shakti@127.0.0.1:5672",
//     "queueName": "err-canta",
//     "exchangeName": "err-cantaHealth",
//     "exchangeType": "direct",
//     "prefetchCount": 1,
//     "options": {}
//   },
//   loggerIns = getLoggerInstance({
//     "name": "ch-rabbitMQ",
//     "streams": [
//       {
//         "level": "debug",
//         "stream": process.stdout
//       }
//     ]
//   }),
//   rabbit = new RabbitMQ(config, loggerIns),
//   count = 0;
//
// setInterval(() => {
//
//   rabbit.publish(JSON.stringify({"msg": `Hello This is cantahealth ${count} !!!!`}))
//     .then(msg => {
//       count++;
//     });
// }, 5000);
// //
// rabbit.on("msgReceived", msg => {
//   console.log("In Ack=======================================>", msg.content.toString());
//
//   rabbit.acknowledgeMessage(msg);
// });
//
// setInterval(() => {
//   rabbit.consume();
// }, 10000);
