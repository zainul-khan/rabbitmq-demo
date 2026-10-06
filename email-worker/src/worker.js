const amqp = require("amqplib");

const RABBITMQ_URL = "amqp://admin:admin@localhost:5672";

const EXCHANGE_NAME = "orders.exchange";
const QUEUE_NAME = "orders.email";

const ROUTING_KEYS = {
    CREATED: "order.created",
    CANCELLED: "order.cancelled",
};

async function startWorker() {
    const connection = await amqp.connect(RABBITMQ_URL);
    const channel = await connection.createChannel();

    // Make sure exchange exists
    await channel.assertExchange(EXCHANGE_NAME, "direct", {
        durable: true,
    });

    // Create email queue
    await channel.assertQueue(QUEUE_NAME, {
        durable: true,
    });

    // Bind queue to order.created
    await channel.bindQueue(
        QUEUE_NAME,
        EXCHANGE_NAME,
        ROUTING_KEYS.CREATED
    );

    // Bind same queue to order.cancelled
    await channel.bindQueue(
        QUEUE_NAME,
        EXCHANGE_NAME,
        ROUTING_KEYS.CANCELLED
    );

    console.log("Connected to RabbitMQ");
    console.log(
        `Waiting for "${ROUTING_KEYS.CREATED}" and "${ROUTING_KEYS.CANCELLED}" messages...`
    );

    channel.consume(QUEUE_NAME, async (message) => {
        if (!message) return;

        try {
            const event = JSON.parse(message.content.toString());

            // Important: identify which routing key delivered the message
            const routingKey = message.fields.routingKey;

            console.log("\n-----------------------------");
            console.log("Email Worker received:");
            console.log("Routing Key:", routingKey);
            console.log("Data:", event);

            if (routingKey === ROUTING_KEYS.CREATED) {
                await sendConfirmationEmail(event);

                console.log(
                    `Confirmation email sent for ${event.orderId}`
                );
            } else if (routingKey === ROUTING_KEYS.CANCELLED) {
                await sendCancellationEmail(event);

                console.log(
                    `Cancellation email sent for ${event.orderId}`
                );
            } else {
                console.log("Unknown routing key:", routingKey);
            }

            // Tell RabbitMQ processing succeeded
            channel.ack(message);

            console.log("Message acknowledged");
        } catch (error) {
            console.error("Email processing failed:", error);

            // For now, remove the message.
            // Later we'll implement retry + DLQ.
            channel.ack(message);
        }
    });
}

function sendConfirmationEmail(order) {
    return new Promise((resolve) => {
        console.log(
            `Sending order confirmation email for ${order.orderId}...`
        );

        setTimeout(resolve, 2000);
    });
}

function sendCancellationEmail(order) {
    return new Promise((resolve) => {
        console.log(
            `Sending order cancellation email for ${order.orderId}...`
        );

        setTimeout(resolve, 2000);
    });
}

startWorker().catch((error) => {
    console.error("Email Worker failed to start:", error);
});