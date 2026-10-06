const amqp = require("amqplib");

const RABBITMQ_URL =
    "amqp://admin:admin@localhost:5672";


/*
|--------------------------------------------------------------------------
| RabbitMQ Configuration
|--------------------------------------------------------------------------
*/

const EXCHANGE_NAME = "orders.exchange";

const QUEUE_NAME = "orders.payment";

const ROUTING_KEY = "order.created";


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
     * 3. Create exchange
     *
     * Exchange:
     *
     * orders.exchange
     *
     * Type:
     *
     * direct
     */
    await channel.assertExchange(
        EXCHANGE_NAME,
        "direct",
        {
            durable: true,
        }
    );


    /*
     * 4. Create queue
     *
     * This is where messages will WAIT.
     */
    await channel.assertQueue(
        QUEUE_NAME,
        {
            durable: true,
        }
    );


    /*
     * 5. Create BINDING
     *
     * This is the connection between:
     *
     * Exchange
     *      ↓
     * Queue
     *
     * And the routing key tells RabbitMQ
     * which messages should go through.
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
     * 6. Consume messages
     */
    channel.consume(
        QUEUE_NAME,
        async (message) => {

            if (!message) {
                return;
            }

            try {

                const order =
                    JSON.parse(
                        message.content.toString()
                    );


                console.log(
                    "\n-----------------------------"
                );

                console.log(
                    "Payment Worker received:"
                );

                console.log(order);


                /*
                 * Simulate payment processing
                 */
                console.log(
                    `Processing payment for ${order.orderId}...`
                );


                await processPayment(order);


                console.log(
                    `Payment successful for ${order.orderId}`
                );


                /*
                 * Tell RabbitMQ:
                 *
                 * "I successfully processed this message."
                 */
                channel.ack(message);


                console.log(
                    "Message acknowledged"
                );

            } catch (error) {

                console.error(
                    "Payment processing failed:",
                    error
                );

                channel.ack(message);
            }
        }
    );
}


/*
|--------------------------------------------------------------------------
| Fake Payment Processing
|--------------------------------------------------------------------------
*/

function processPayment(order) {

    return new Promise((resolve) => {

        setTimeout(() => {

            resolve();

        }, 3000);

    });
}


/*
|--------------------------------------------------------------------------
| Start Worker
|--------------------------------------------------------------------------
*/

startWorker().catch((error) => {

    console.error(
        "Worker failed to start:",
        error
    );

});