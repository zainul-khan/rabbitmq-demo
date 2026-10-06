const amqp = require("amqplib");

const RABBITMQ_URL =
    "amqp://admin:admin@localhost:5672";


/*
|--------------------------------------------------------------------------
| RabbitMQ Configuration
|--------------------------------------------------------------------------
*/

// SAME exchange as the API and Payment Worker
const EXCHANGE_NAME = "orders.exchange";

// NEW queue specifically for cancellation
const QUEUE_NAME = "orders.cancellation";

// NEW routing key
const ROUTING_KEY = "order.cancelled";


async function startWorker() {

    /*
     * 1. Connect to RabbitMQ
     */
    const connection =
        await amqp.connect(RABBITMQ_URL);


    /*
     * 2. Create channel
     */
    const channel =
        await connection.createChannel();


    /*
     * 3. Create the exchange
     *
     * This is the SAME exchange used
     * by the Payment Worker.
     */
    await channel.assertExchange(
        EXCHANGE_NAME,
        "direct",
        {
            durable: true,
        }
    );


    /*
     * 4. Create cancellation queue
     *
     * This queue is different from:
     *
     * orders.payment
     */
    await channel.assertQueue(
        QUEUE_NAME,
        {
            durable: true,
        }
    );


    /*
     * 5. Bind the queue to the exchange
     *
     * We are saying:
     *
     * orders.exchange
     *       +
     * order.cancelled
     *       ↓
     * orders.cancellation
     */
    await channel.bindQueue(
        QUEUE_NAME,
        EXCHANGE_NAME,
        ROUTING_KEY
    );


    console.log("Connected to RabbitMQ");

    console.log(
        `Waiting for "${ROUTING_KEY}" messages...`
    );


    /*
     * 6. Start consuming messages
     */
    channel.consume(
        QUEUE_NAME,
        async (message) => {

            if (!message) {
                return;
            }

            try {

                /*
                 * Convert RabbitMQ Buffer
                 * into JavaScript object
                 */
                const cancellation =
                    JSON.parse(
                        message.content.toString()
                    );


                console.log(
                    "\n-----------------------------"
                );

                console.log(
                    "Cancellation Worker received:"
                );

                console.log(cancellation);


                /*
                 * Simulate cancellation processing
                 */
                console.log(
                    `Processing cancellation for ${cancellation.orderId}...`
                );


                await processCancellation(
                    cancellation
                );


                console.log(
                    `Order ${cancellation.orderId} cancelled successfully`
                );


                /*
                 * Tell RabbitMQ:
                 *
                 * "I successfully processed
                 *  this message."
                 */
                channel.ack(message);


                console.log(
                    "Message acknowledged"
                );

            } catch (error) {

                console.error(
                    "Cancellation processing failed:",
                    error
                );

                /*
                 * For now we're acknowledging
                 * even on failure.
                 *
                 * Later we'll implement:
                 *
                 * retry + dead letter queue
                 */
                channel.ack(message);
            }
        }
    );
}


function processCancellation(cancellation) {

    return new Promise((resolve) => {

        setTimeout(() => {

            resolve();

        }, 2000);

    });
}


/*
|--------------------------------------------------------------------------
| Start Worker
|--------------------------------------------------------------------------
*/

startWorker().catch((error) => {

    console.error(
        "Cancellation Worker failed to start:",
        error
    );

});