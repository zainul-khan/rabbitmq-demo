const amqp = require("amqplib");

const RABBITMQ_URL = "amqp://admin:admin@localhost:5672";

// Main
const MAIN_EXCHANGE = "orders.exchange";
const MAIN_QUEUE = "orders.payment";
const ROUTING_KEY = "order.created";

// Retry
const RETRY_EXCHANGE = "orders.payment.retry.exchange";
const RETRY_QUEUE = "orders.payment.retry";

// DLQ
const DLX_EXCHANGE = "orders.dlx";
const DLQ_QUEUE = "orders.payment.dlq";

const MAX_RETRIES = 3;
const RETRY_DELAY = 5000;

async function startWorker() {
    const connection = await amqp.connect(RABBITMQ_URL);
    const channel = await connection.createChannel();

    /*
     * Main exchange
     */
    await channel.assertExchange(
        MAIN_EXCHANGE,
        "direct",
        { durable: true }
    );

    /*
     * Retry exchange
     */
    await channel.assertExchange(
        RETRY_EXCHANGE,
        "direct",
        { durable: true }
    );

    /*
     * DLX exchange
     */
    await channel.assertExchange(
        DLX_EXCHANGE,
        "direct",
        { durable: true }
    );

    /*
     * Main payment queue
     *
     * If message is rejected,
     * RabbitMQ sends it to RETRY_EXCHANGE.
     */
    await channel.assertQueue(
        MAIN_QUEUE,
        {
            durable: true,

            arguments: {
                "x-dead-letter-exchange": RETRY_EXCHANGE,
                "x-dead-letter-routing-key": ROUTING_KEY,
            },
        }
    );

    /*
     * Retry queue
     *
     * Message stays here for 5 seconds.
     *
     * After TTL expires, RabbitMQ sends
     * it back to orders.exchange.
     */
    await channel.assertQueue(
        RETRY_QUEUE,
        {
            durable: true,

            arguments: {
                "x-message-ttl": RETRY_DELAY,

                "x-dead-letter-exchange": MAIN_EXCHANGE,
                "x-dead-letter-routing-key": ROUTING_KEY,
            },
        }
    );

    /*
     * DLQ
     */
    await channel.assertQueue(
        DLQ_QUEUE,
        {
            durable: true,
        }
    );

    /*
     * Main queue -> main exchange
     */
    await channel.bindQueue(
        MAIN_QUEUE,
        MAIN_EXCHANGE,
        ROUTING_KEY
    );

    /*
     * Retry queue -> retry exchange
     */
    await channel.bindQueue(
        RETRY_QUEUE,
        RETRY_EXCHANGE,
        ROUTING_KEY
    );

    /*
     * DLQ -> DLX
     */
    await channel.bindQueue(
        DLQ_QUEUE,
        DLX_EXCHANGE,
        ROUTING_KEY
    );

    console.log("Payment Worker connected");
    console.log("Waiting for payments...");

    channel.consume(MAIN_QUEUE, async (message) => {
        if (!message) return;

        const order = JSON.parse(
            message.content.toString()
        );

        const retryCount = getRetryCount(message);

        console.log("\n-----------------------------");
        console.log("Payment Worker received:");
        console.log(order);

        console.log(`Retry count: ${retryCount}`);

        try {
            await processPayment(order);

            console.log("Payment successful");

            channel.ack(message);

        } catch (error) {

            console.error(
                "Payment failed:",
                error.message
            );

            if (retryCount < MAX_RETRIES) {

                console.log(
                    `Retrying in ${RETRY_DELAY / 1000}s...`
                );

                /*
                 * false = don't acknowledge
                 * false = don't requeue directly
                 *
                 * Because the queue has a DLX,
                 * RabbitMQ sends it to retry queue.
                 */
                channel.nack(
                    message,
                    false,
                    false
                );

            } else {

                console.log(
                    "Maximum retries reached."
                );

                /*
                 * Send message to DLQ.
                 */
                channel.publish(
                    DLX_EXCHANGE,
                    ROUTING_KEY,
                    message.content,
                    {
                        persistent: true,
                        headers: {
                            "x-final-retry-count": retryCount,
                        },
                    }
                );

                /*
                 * Remove original message
                 * from main queue.
                 */
                channel.ack(message);

                console.log(
                    "Message moved to DLQ"
                );
            }
        }
    });
}


/*
 * RabbitMQ adds x-death information
 * whenever a message is dead-lettered.
 */
function getRetryCount(message) {

    const xDeath = message.properties.headers?.["x-death"];

    if (!xDeath) {
        return 0;
    }

    const mainQueueDeath = xDeath.find(
        (entry) => entry.queue === MAIN_QUEUE
    );

    return mainQueueDeath
        ? mainQueueDeath.count
        : 0;
}


/*
 * Simulate payment processing.
 *
 * For now we intentionally fail
 * so we can observe retries.
 */
function processPayment(order) {

    return new Promise((resolve, reject) => {

        setTimeout(() => {

            reject(
                new Error("Payment gateway failed")
            );

        }, 1000);

    });
}


startWorker().catch((error) => {
    console.error(
        "Payment Worker failed to start:",
        error
    );
});