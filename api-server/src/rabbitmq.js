const amqp = require("amqplib");

const RABBITMQ_URL = "amqp://admin:admin@localhost:5672";

let channel;

async function connectRabbitMQ() {
    const connection = await amqp.connect(RABBITMQ_URL);
    channel = await connection.createChannel();
    console.log("Connected to RabbitMQ");
}

function getChannel() {
    if (!channel) {
        throw new Error("RabbitMQ channel is not initialized");
    }
    return channel;
}

module.exports = {
    connectRabbitMQ,
    getChannel,
};