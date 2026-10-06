
const express = require("express");
const {
    connectRabbitMQ,
    getChannel,
} = require("./rabbitmq");

const app = express();

app.use(express.json());

const PORT = 3000;

/*
|--------------------------------------------------------------------------
| RabbitMQ Configuration
|--------------------------------------------------------------------------
|
| EXCHANGE_NAME
|     ↓
|     WHERE do I send the message?
|
| ROUTING_KEY
|     ↓
|     WHAT TYPE of message is this?
|
*/

const EXCHANGE_NAME = "orders.exchange";

// Different types of events
const ROUTING_KEYS = {
    CREATED: "order.created",
    CANCELLED: "order.cancelled",
    COMPLETED: "order.completed",
};


/*
|--------------------------------------------------------------------------
| CREATE ORDER
|--------------------------------------------------------------------------
*/

app.post("/orders", async (req, res) => {
    try {
        const { userId, productId, amount } = req.body;

        const order = {
            orderId: `ORD-${Date.now()}`,
            userId,
            productId,
            amount,
        };

        const channel = getChannel();

        /*
         * EXCHANGE
         * --------
         *
         * We create ONE exchange.
         *
         * The exchange is responsible for
         * routing messages to queues.
         */
        await channel.assertExchange(
            EXCHANGE_NAME,
            "direct",
            {
                durable: true,
            }
        );

        /*
         * ROUTING KEY
         * -----------
         *
         * This tells RabbitMQ:
         *
         * "This is an order.created event"
         */
        channel.publish(
            EXCHANGE_NAME,

            ROUTING_KEYS.CREATED,

            Buffer.from(JSON.stringify(order)),

            {
                persistent: true,
            }
        );

        console.log(
            `Published event: ${ROUTING_KEYS.CREATED}`
        );

        console.log("Order:", order);

        res.status(202).json({
            message: "Order received",
            order,
        });

    } catch (error) {
        console.error(error);

        res.status(500).json({
            message: "Failed to create order",
        });
    }
});


/*
|--------------------------------------------------------------------------
| CANCEL ORDER
|--------------------------------------------------------------------------
*/

app.post("/orders/:orderId/cancel", async (req, res) => {
    try {

        const { orderId } = req.params;

        const channel = getChannel();

        const cancellationEvent = {
            orderId,
            cancelledAt: new Date().toISOString(),
        };

        /*
         * SAME EXCHANGE
         *
         * But DIFFERENT routing key.
         */
        channel.publish(
            EXCHANGE_NAME,

            ROUTING_KEYS.CANCELLED,

            Buffer.from(
                JSON.stringify(cancellationEvent)
            ),

            {
                persistent: true,
            }
        );

        console.log(
            `Published event: ${ROUTING_KEYS.CANCELLED}`
        );

        res.json({
            message: "Order cancellation event published",
            orderId,
        });

    } catch (error) {
        console.error(error);

        res.status(500).json({
            message: "Failed to cancel order",
        });
    }
});


/*
|--------------------------------------------------------------------------
| START SERVER
|--------------------------------------------------------------------------
*/

async function startServer() {

    await connectRabbitMQ();

    app.listen(PORT, () => {
        console.log(
            `API server running on http://localhost:${PORT}`
        );
    });
}

startServer();
