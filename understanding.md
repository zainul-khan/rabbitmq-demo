RABBITMQ — UPDATED UNDERSTANDING
================================

Project:
rabbitmq-demo

Architecture:
- API Server
- Payment Worker
- Email Worker
- Cancellation Worker
- RabbitMQ


1. BASIC RABBITMQ FLOW
----------------------

The overall flow is:

Producer
   ↓
Exchange
   ↓
Routing Key + Binding
   ↓
Queue
   ↓
Consumer
   ↓
ACK


In our project:

API Server
   ↓ publish()
orders.exchange
   ↓
Routing Key
   ↓
Binding
   ↓
Queue
   ↓
Worker
   ↓
ACK


2. POST OFFICE ANALOGY
----------------------

RabbitMQ can be understood using a Post Office analogy.

Producer
= Person sending a letter

Exchange
= Post Office / routing center

Routing Key
= Information on the letter telling where/type of delivery

Binding
= Routing rule that says which mailbox should receive which type of letter

Queue
= Mailbox / waiting area where messages wait

Consumer
= Worker/person who picks up messages from the mailbox

ACK
= Confirmation that the worker successfully processed the letter


Example:

API Server
    |
    | publish("orders.exchange", "order.created", message)
    ↓
orders.exchange
    |
    | checks routing key: "order.created"
    ↓
Binding Rule
    |
    ↓
orders.payment
    |
    ↓
Payment Worker
    |
    | ACK
    ↓
Message completed


3. EXCHANGE
-----------

Our exchange is:

orders.exchange

We use a direct exchange:

await channel.assertExchange(
    EXCHANGE_NAME,
    "direct",
    { durable: true }
);


The exchange is responsible for ROUTING messages.

The exchange itself does NOT normally store messages.

It receives a message and checks:

"What routing key does this message have?"

Then it checks its bindings and decides which queues should receive the message.


4. WHY assertExchange() IS IN EVERY NODE PROJECT
-------------------------------------------------

We have:

API Server
Payment Worker
Email Worker
Cancellation Worker

And all of them contain:

await channel.assertExchange(
    "orders.exchange",
    "direct",
    { durable: true }
);


This does NOT mean every application creates a separate exchange.

assertExchange() means:

"RabbitMQ, make sure this exchange exists with this configuration."


If the exchange already exists:
- RabbitMQ does not create another one.
- The application simply continues.

If it does not exist:
- RabbitMQ creates it.


Therefore all applications can safely call:

assertExchange("orders.exchange", ...)


Important:

All applications are connecting to the SAME RabbitMQ server and the SAME exchange.

They are not creating separate exchanges with the same name.


5. WHY channel.publish() IS ONLY IN server.js
---------------------------------------------

Currently, our API Server is the PRODUCER.

The API Server creates an order and publishes an event:

channel.publish(
    EXCHANGE_NAME,
    ROUTING_KEYS.CREATED,
    Buffer.from(JSON.stringify(order)),
    { persistent: true }
);


The current architecture is:

API Server
    ↓
Producer
    ↓
publish()
    ↓
orders.exchange


The workers are currently CONSUMERS.

Payment Worker:
    consume()

Email Worker:
    consume()

Cancellation Worker:
    consume()


Therefore, only server.js currently calls:

channel.publish()


IMPORTANT:

RabbitMQ does NOT require only the API Server to publish.

A worker CAN also be both:

Consumer + Producer


For example, later we could have:

API Server
    ↓
order.created
    ↓
Payment Worker
    ↓
payment processing
    ↓
payment.completed
    ↓
orders.exchange
    ↓
Email Worker


In that case Payment Worker would:

1. Consume a message.
2. Process it.
3. Publish another event.


6. QUEUE
--------

A queue is where messages wait until a consumer processes them.

Our queues are:

orders.payment
orders.email
orders.cancellation


For example:

orders.payment
    ↓
Payment Worker


The queue belongs to the consumer side of the system.

The queue stores messages until they are delivered to a consumer.


7. WHY assertQueue() IS CURRENTLY ONLY IN WORKERS
-------------------------------------------------

Payment Worker has:

await channel.assertQueue(
    QUEUE_NAME,
    { durable: true }
);


Email Worker has:

await channel.assertQueue(
    QUEUE_NAME,
    { durable: true }
);


Cancellation Worker has:

await channel.assertQueue(
    QUEUE_NAME,
    { durable: true }
);


The reason is architectural.

Each worker is responsible for consuming from its own queue.

Payment Worker:
    orders.payment

Email Worker:
    orders.email

Cancellation Worker:
    orders.cancellation


So each worker ensures that the queue it depends on exists.


IMPORTANT:

Technically, the API Server COULD create the queues.

RabbitMQ does not prevent this.

But keeping queue configuration close to the worker that consumes from it makes the architecture easier to understand and maintain.


8. BINDING
----------

A binding creates a routing rule between:

Exchange
+
Routing Key
+
Queue


Example:

await channel.bindQueue(
    "orders.payment",
    "orders.exchange",
    "order.created"
);


This means:

"If orders.exchange receives a message with routing key order.created,
route that message to orders.payment."


Think:

Exchange + Routing Key → Queue


Binding is the ROUTING RULE.


9. CONSUMER
-----------

After the queue exists and is bound to the exchange, the worker consumes messages:

channel.consume(
    QUEUE_NAME,
    async (message) => {
        ...
    }
);


This means:

"Give this worker messages from this queue."


For example:

orders.payment
    ↓
channel.consume()
    ↓
Payment Worker


The consumer is responsible for processing the message.


10. ACK
-------

After successful processing:

channel.ack(message);


ACK means:

"I successfully processed this message."


RabbitMQ can then remove the message from the queue.


Current simplified flow:

Message
   ↓
Consumer
   ↓
Process
   ↓
ACK
   ↓
Message removed


Later we will learn:

NACK
Retry
Dead Letter Queue (DLQ)


11. ROUTING KEY
---------------

A routing key is a string attached to a published message.

Example:

order.created

or:

order.cancelled


The producer publishes:

channel.publish(
    EXCHANGE_NAME,
    "order.created",
    message
);


The direct exchange then looks at:

order.created


It compares this against queue bindings.


For example:

orders.payment
    binding = order.created

orders.email
    binding = order.created

orders.cancellation
    binding = order.cancelled


If the message has:

order.created


Then:

orders.payment     → receives it
orders.email       → receives it
orders.cancellation → does NOT receive it


12. SAME ROUTING KEY CAN GO TO MULTIPLE QUEUES
----------------------------------------------

This is an important concept.

We can have:

orders.exchange
       |
       | order.created
       |
       +------------------+
       |                  |
       ↓                  ↓
orders.payment      orders.email
       |                  |
       ↓                  ↓
Payment Worker       Email Worker


Both queues have a binding for:

order.created


Therefore both queues receive their own copy of the message.


This is NOT the same as having two workers consume from the same queue.

Two workers on the SAME queue compete for messages.

Two DIFFERENT queues bound to the same routing key each receive the message.


13. EMAIL WORKER HAS TWO ROUTING KEYS
-------------------------------------

Our Email Worker currently has:

const ROUTING_KEYS = {
    CREATED: "order.created",
    CANCELLED: "order.cancelled",
};


And binds the SAME queue twice:

await channel.bindQueue(
    QUEUE_NAME,
    EXCHANGE_NAME,
    ROUTING_KEYS.CREATED
);


await channel.bindQueue(
    QUEUE_NAME,
    EXCHANGE_NAME,
    ROUTING_KEYS.CANCELLED
);


Therefore:

orders.exchange
       |
       +---- order.created ------+
       |                         |
       +---- order.cancelled ----+
                                 |
                                 ↓
                           orders.email
                                 |
                                 ↓
                           Email Worker


The Email Worker can receive both events.


14. CURRENT PROJECT CONSTANTS
-----------------------------

API SERVER:

const EXCHANGE_NAME = "orders.exchange";

const ROUTING_KEYS = {
    CREATED: "order.created",
    CANCELLED: "order.cancelled",
    COMPLETED: "order.completed",
};


The API Server needs:
- Exchange name
- Routing keys it can publish


PAYMENT WORKER:

const EXCHANGE_NAME = "orders.exchange";
const QUEUE_NAME = "orders.payment";
const ROUTING_KEY = "order.created";


Payment Worker needs:
- Exchange name
- Its queue name
- Routing key it wants to receive


EMAIL WORKER:

const EXCHANGE_NAME = "orders.exchange";
const QUEUE_NAME = "orders.email";

const ROUTING_KEYS = {
    CREATED: "order.created",
    CANCELLED: "order.cancelled",
};


Email Worker needs:
- Exchange name
- Its queue name
- Multiple routing keys it wants to receive


CANCELLATION WORKER:

const EXCHANGE_NAME = "orders.exchange";
const QUEUE_NAME = "orders.cancellation";
const ROUTING_KEY = "order.cancelled";


Cancellation Worker needs:
- Exchange name
- Its queue name
- Routing key it wants to receive


15. CURRENT ARCHITECTURE
------------------------

                         API SERVER
                          PRODUCER
                             |
                             | publish()
                             ↓
                    +-------------------+
                    | orders.exchange   |