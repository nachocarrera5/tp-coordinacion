import pika
from functools import partial
from contextlib import contextmanager
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange, MessageMiddlewareMessageError, MessageMiddlewareDisconnectedError, MessageMiddlewareCloseError, MessageMiddlewareDeleteError

@contextmanager
def _pika_error_to_middleware_error(operation, error_type, connection_error=MessageMiddlewareDisconnectedError):

    try:
        yield
    except pika.exceptions.AMQPConnectionError as e:
        raise connection_error(f"Error connecting to RabbitMQ during {operation}: {e}") from e
    except pika.exceptions.AMQPError as e:
        raise error_type(f"Error during {operation} in RabbitMQ: {e}") from e

class _MessageMiddlewareRabbitMQBase:

    def __init__(self,host):

        with _pika_error_to_middleware_error("initialization", MessageMiddlewareMessageError):
            self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
            self._channel = self._connection.channel()

    def _check_connection(self):

        if self._connection.is_closed:
            raise MessageMiddlewareDisconnectedError("Connection to RabbitMQ is closed.")

        if self._channel.is_closed:
            raise MessageMiddlewareMessageError("Channel to RabbitMQ is closed.")

    def start_consuming(self, on_message_callback):

        self._check_connection()

        def callback(ch, method, _properties, body): #pika llama al callback con 4 parametros

            ack = partial(ch.basic_ack, delivery_tag=method.delivery_tag)
            nack = partial(ch.basic_nack, delivery_tag=method.delivery_tag)

            on_message_callback(body, ack, nack)

        with _pika_error_to_middleware_error("consuming messages", MessageMiddlewareMessageError):
            self._channel.basic_qos(prefetch_count=1) # linea ausente en tp mom, rabbit adelanta mensajes y hace que falle el escenario 4
            self._channel.basic_consume(queue=self._queue_name, on_message_callback=callback, auto_ack=False) # para queue_name, la cola asigna el nombre recibido, y el exchange, asigna el nombre de la cola generada aleatoriamente y guardada en self._queue_name
            self._channel.start_consuming()

    def stop_consuming(self):

        self._check_connection()

        if not self._channel.consumer_tags:
            return

        with _pika_error_to_middleware_error("stopping consumption", MessageMiddlewareMessageError):
            self._channel.stop_consuming()

    def close(self):

        if self._connection.is_closed:
            return

        with _pika_error_to_middleware_error("closing connection", MessageMiddlewareCloseError, MessageMiddlewareCloseError):
            self._connection.close()

class MessageMiddlewareQueueRabbitMQ(_MessageMiddlewareRabbitMQBase, MessageMiddlewareQueue):

    def __init__(self, host, queue_name):

        self._queue_name = queue_name

        super().__init__(host)

        with _pika_error_to_middleware_error("queue initialization", MessageMiddlewareMessageError):
            self._channel.queue_declare(queue=queue_name, durable=False)


    def send(self, message):

        self._check_connection()

        with _pika_error_to_middleware_error("sending message", MessageMiddlewareMessageError):
            self._channel.basic_publish(exchange="", routing_key=self._queue_name, body=message)



class MessageMiddlewareExchangeRabbitMQ(_MessageMiddlewareRabbitMQBase, MessageMiddlewareExchange):
    def __init__(self, host, exchange_name, routing_keys):

        self._exchange_name = exchange_name
        self._routing_keys = routing_keys

        super().__init__(host)

        with _pika_error_to_middleware_error("exchange initialization", MessageMiddlewareMessageError):

            self._channel.exchange_declare(exchange=exchange_name, exchange_type="direct", durable=False)
            queue = self._channel.queue_declare(queue="", exclusive=True)
            self._queue_name = queue.method.queue

            for routing_key in self._routing_keys:
                self._channel.queue_bind(exchange=exchange_name, queue=self._queue_name, routing_key=routing_key)


    def send(self, message):

        self._check_connection()

        with _pika_error_to_middleware_error("sending message", MessageMiddlewareMessageError):
            for routing_key in self._routing_keys:
                self._channel.basic_publish(exchange=self._exchange_name, routing_key=routing_key, body=message)
                