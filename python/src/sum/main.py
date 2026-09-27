import os
import logging
import threading

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )

        self.control_exchange_input = middleware.MessageMiddlewareExchangeRabbitMQ( # cola para la replica y el exchange de control
            MOM_HOST, SUM_CONTROL_EXCHANGE, [f"{SUM_CONTROL_EXCHANGE}"]
        )

        self.control_exchange_output = middleware.MessageMiddlewareExchangeRabbitMQ( # otra igual para publicar porque la otra queda bloqueadaxw
            MOM_HOST, SUM_CONTROL_EXCHANGE, [f"{SUM_CONTROL_EXCHANGE}"]
        )

        self.data_output_exchanges = []

        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)

        self.amount_by_client = {}
        self.lock = threading.Lock()

    def _process_data(self, fruit, amount, client_id):

        logging.info(f"Process data")

        with self.lock:

            amount_by_fruit = self.amount_by_client.setdefault(client_id, {})

            current = amount_by_fruit.get(fruit, fruit_item.FruitItem(fruit, 0))

            amount_by_fruit[fruit] = current + fruit_item.FruitItem(fruit, int(amount))

    def _process_eof(self, client_id):

        logging.info(f"Broadcasting data messages")

        with self.lock:
            amount_by_fruit = self.amount_by_client.pop(client_id, {})

        for final_fruit_item in amount_by_fruit.values():
            message = message_protocol.internal.serialize(client_id, message_protocol.internal.FRUITS, [final_fruit_item.fruit, final_fruit_item.amount])

            for data_output_exchange in self.data_output_exchanges:
                data_output_exchange.send(message)

        logging.info(f"Broadcasting EOF message")
        eof_message = message_protocol.internal.serialize(client_id, message_protocol.internal.EOF, None)
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(eof_message)


    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)

        client_id = fields[message_protocol.internal.ID]
        message_type = fields[message_protocol.internal.TYPE]
        payload = fields[message_protocol.internal.PAYLOAD]

        if message_type == message_protocol.internal.FRUITS:
            fruit, amount = payload
            self._process_data(fruit, amount, client_id)

        elif message_type == message_protocol.internal.EOF:
            self.control_exchange_output.send(message) # con esto el eof lo reciben todas las replicas

        else:
            logging.warning(f"Unknown message type: {message_type}")
            nack()
            return

        ack()

    def process_control_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)

        client_id = fields[message_protocol.internal.ID]
        message_type = fields[message_protocol.internal.TYPE]

        if message_type != message_protocol.internal.EOF:
            logging.warning("Wrong control message type")
            nack()
            return

        self._process_eof(client_id)
        ack()

    def start(self):

        control_thread = threading.Thread(target=self.control_exchange_input.start_consuming, args=(self.process_control_message,), daemon=True)
        control_thread.start()

        self.input_queue.start_consuming(self.process_data_messsage)

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
