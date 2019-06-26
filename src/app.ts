import * as express from "express";
import * as config from "config";
import * as _ from "underscore";
import { CurrencyPairTradesCounter } from "./currency_pair";
import { BinanceMarket } from "./markets/binance";
import { Monitor } from "./monitor";
import { AmqpSender } from "./amqp";

// Create Express server
const app = express();

const MAX_EXPORTED_SECONDS = 20 * 60; // 20 minutes

app.get('/', function(req, res) {
  res.setHeader('Content-Type', 'text/csv');

  const symbols = _.keys(monitor.currencyPairs);

  symbols.forEach((symbol: string) => {
    res.write(',');
    res.write(symbol);
  });
  res.write('\r\n');

  let oldestBucketStored = Number.MAX_SAFE_INTEGER;
  let newestBucketStored = 0;
  symbols.forEach((symbol: string) => {
    oldestBucketStored = Math.min(
      monitor.currencyPairs[symbol].oldestBucketStored || Number.MAX_SAFE_INTEGER, 
      oldestBucketStored,
    );
    newestBucketStored = Math.max(
      monitor.currencyPairs[symbol].newestBucketStored || 0,
      newestBucketStored,
    );
  });
  console.log(oldestBucketStored, newestBucketStored);

  for (let bucket = oldestBucketStored; bucket <= newestBucketStored; bucket += 1) {
    res.write(`${bucket}`);
    
    symbols.forEach((symbol: string) => {
      const currencyPair = monitor.currencyPairs[symbol];
      const messageRate = currencyPair.countTradesInBucket(bucket);
      res.write(',');
      res.write(`${messageRate}`);
    });
    res.write('\r\n');
  }
  
  res.end();
});

app.listen(3005);

const rabbitConf = config.get<{url: string, queue: string}>("rabbitmq");
const publisher = new AmqpSender(rabbitConf.url, rabbitConf.queue);
const monitor = new Monitor(publisher);
publisher
.init()
.then(() => initMonitor());

function initMonitor() {
  const binance = new BinanceMarket();
  config.get<string[]>("currencyPairs").forEach(symbol => {
    const currencyPair = new CurrencyPairTradesCounter(symbol, binance);
    monitor.addCurrencyPairTradeCounter(currencyPair);
  });
  monitor.startMonitoring();
}


var amqp = require('amqplib/callback_api');
amqp.connect('amqp://localhost', function(error0:any, connection:any) {
    if (error0) {
        throw error0;
    }
    connection.createChannel(function(error1:any, channel:any) {
        if (error1) {
            throw error1;
        }

        var queue = rabbitConf.queue;

        channel.assertQueue(queue, {
            durable: false
        });

        console.log(" [*] Waiting for messages in %s. To exit press CTRL+C", queue);

        channel.consume(queue, function(msg:any) {
            console.log(" [x] Received %s", msg.content.toString());
        }, {
            noAck: true
        });
    });
});

