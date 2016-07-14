/**
 * Created by shakti on 16/3/16.
 */

var config = {
  "rabbitmq": {
    "url": "amqp://integration:integration@10.18.6.109:5672",
    "queueName": "test-canta",
    "exchangeName": "test-cantaHealth",
    "exchangeType": "direct",
    "prefetchCount": 1,
    "options": {}
  }
};

module.exports = config;
