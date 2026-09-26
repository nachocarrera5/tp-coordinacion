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
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)
        self.amount_by_client = {}

    def _process_data(self, fruit, amount, client_id):

        logging.info(f"Process data")

        amount_by_fruit = self.amount_by_client.setdefault(client_id, {})

        current = amount_by_fruit.get(fruit, fruit_item.FruitItem(fruit, 0))

        amount_by_fruit[fruit] = current + fruit_item.FruitItem(fruit, int(amount))

    def _process_eof(self, client_id):

        logging.info(f"Broadcasting data messages")

        amount_by_fruit = self.amount_by_client.get(client_id, {})

        for final_fruit_item in amount_by_fruit.values():
            message = message_protocol.internal.serialize(client_id, message_protocol.internal.FRUITS, [final_fruit_item.fruit, final_fruit_item.amount])

            for data_output_exchange in self.data_output_exchanges:
                data_output_exchange.send(message)

        logging.info(f"Broadcasting EOF message")
        eof_message = message_protocol.internal.serialize(client_id, message_protocol.internal.EOF, None)
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(eof_message)

        self.amount_by_client.pop(client_id, None)


    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)

        client_id = fields[message_protocol.internal.ID]
        message_type = fields[message_protocol.internal.TYPE]
        payload = fields[message_protocol.internal.PAYLOAD]

        if message_type == message_protocol.internal.FRUITS:
            fruit, amount = payload
            self._process_data(fruit, amount, client_id)

        elif message_type == message_protocol.internal.EOF:
            self._process_eof(client_id)

        else:
            logging.warning(f"Unknown message type: {message_type}")
            nack()
            return

        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_data_messsage)

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
