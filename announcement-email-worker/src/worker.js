const amqp = require("amqplib");

const RABBITMQ_URL = "amqp://admin:admin@localhost:5672";

const EXCHANGE_NAME = "announcements.exchange";
const QUEUE_NAME = "announcements.email";

async function startWorker() {
    const connection = await amqp.connect(RABBITMQ_URL);
    const channel = await connection.createChannel();

    await channel.assertExchange(
        EXCHANGE_NAME,
        "fanout",
        { durable: true }
    );

    await channel.assertQueue(
        QUEUE_NAME,
        { durable: true }
    );

    await channel.bindQueue(
        QUEUE_NAME,
        EXCHANGE_NAME,
        ""
    );

    console.log("Email Worker connected");
    console.log("Waiting for announcements...");

    channel.consume(QUEUE_NAME, async (message) => {
        if (!message) return;

        try {
            const announcement = JSON.parse(
                message.content.toString()
            );

            console.log("\n📧 Email Worker");
            console.log("Sending announcement email:");
            console.log(announcement);

            await sendEmail(announcement);

            channel.ack(message);

            console.log("Email announcement processed");
        } catch (error) {
            console.error("Processing failed:", error);

            channel.ack(message);
        }
    });
}

function sendEmail(announcement) {
    return new Promise((resolve) => {
        setTimeout(resolve, 1000);
    });
}

startWorker().catch(console.error);