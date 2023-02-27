require('dotenv').config();
// Access the callback-based API
const amqp = require('amqplib');
console.log('process.env.CLOUDAMQP_URL: ', process.env.CLOUDAMQP_URL);
console.log('process.env.CLOUDAMQP_USERNAME: ', process.env.CLOUDAMQP_USERNAME);
console.log('process.env.CLOUDAMQP_PASSWORD: ', process.env.CLOUDAMQP_PASSWORD);
// if the connection is closed or fails to be established, it will reconnect
let amqpConn = null;
function start() {
    amqp.connect(process.env.CLOUDAMQP_URL, {
        heartbeat: 60,
    }).then((conn) => {
        conn.on("error", function (err) {
            if (err.message !== "Connection closing") {
                console.error("[AMQP] conn error", err.message);
            }
        });
        conn.on("close", function () {
            console.error("[AMQP] reconnecting");
            return setTimeout(start, 1000);
        });
        console.log("[AMQP] connected");
        amqpConn = conn;
        whenConnected();
    }).catch(err => {
        if (err) {
            console.error("[AMQP] exception occurred in creating connection: ", err.message);
            return setTimeout(start, 1000);
        }
    });
}
function whenConnected() {
    startPublisher();
    startWorker();
}
let pubChannel = null;
const offlinePubQueue = [];
function startPublisher() {
    amqpConn.createConfirmChannel().then((ch) => {
        ch.on("error", function (err) {
            console.error("[AMQP] confirm channel error", err.message);
        });
        ch.on("close", function () {
            console.log("[AMQP] confirm channel closed");
        });
        pubChannel = ch;
        while (true) {
            const m = offlinePubQueue.shift();
            if (!m) break;
            publish(m[0], m[1], m[2]);
        }
    }).catch(err => {
        if (closeOnErr(err)) return;
    });
}
// method to publish a message, will queue messages internally if the connection is
//    down and resend later
function publish(exchange, routingKey, content) {
    try {
        pubChannel.publish(exchange, routingKey, content, { persistent: true },
            function (err, ok) {
                console.log('Arguments: ', err, ok);
                if (err) {
                    console.error("[AMQP] publish", err);
                    offlinePubQueue.push([exchange, routingKey, content]);
                    pubChannel.connection.close();
                }
            });
    } catch (e) {
        console.error("[AMQP] publish", e.message);
        offlinePubQueue.push([exchange, routingKey, content]);
    }
}
// A worker that acks messages only if processed succesfully
function startWorker() {
    amqpConn.createChannel().then(ch => {
        ch.on("error", function (err) {
            console.error("[AMQP] channel error", err.message);
        });
        ch.on("close", function () {
            console.log("[AMQP] channel closed");
        });
        ch.prefetch(10);
        ch.assertQueue("jobs", { durable: true }).then(_ok => {
            ch.consume("jobs", processMsg, { noAck: false });
            console.log("Worker is started");
        }).catch(err => {
            if (closeOnErr(err)) return;
        });
        function processMsg(msg) {
            work(msg, function (ok) {
                try {
                    if (ok)
                        ch.ack(msg);
                    else
                        ch.reject(msg, true);
                } catch (e) {
                    closeOnErr(e);
                }
            });
        }
    }).catch(err => {
        if (closeOnErr(err)) return;
    });
}
function work(msg, cb) {
    console.log("Got msg ", msg.content.toString());
    cb(true);
}
function closeOnErr(err) {
    if (!err) return false;
    console.error("[AMQP] error", err);
    amqpConn.close();
    return true;
}
setInterval(function () {
    publish("", "jobs", new Buffer.from("work work work"));
}, 1000);

start();




