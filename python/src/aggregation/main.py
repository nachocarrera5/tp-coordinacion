import os
import logging
import bisect

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.fruits_by_client = {}

    def _process_data(self, fruit, amount, client_id):

        logging.info("Processing data message")

        amount_by_fruit = self.fruits_by_client.setdefault(client_id, {})

        current = amount_by_fruit.get(fruit, fruit_item.FruitItem(fruit, 0))

        amount_by_fruit[fruit] = current + fruit_item.FruitItem(fruit, int(amount))

    def _process_eof(self, client_id): # reordeno cuando recibo EOF

        logging.info("Received EOF")

        amount_by_fruit = self.fruits_by_client.get(client_id, {})

        top_fruits = sorted(amount_by_fruit.values(), reverse=True)[:TOP_SIZE]
        
        result = [
            [item.fruit, item.amount]
            for item in top_fruits
        ]

        message = message_protocol.internal.serialize(client_id, message_protocol.internal.RESULT, result)

        self.output_queue.send(message)

        self.fruits_by_client.pop(client_id, None)

    def process_messsage(self, message, ack, nack):

        logging.info("Process message")

        fields = message_protocol.internal.deserialize(message)

        client_id = fields[message_protocol.internal.ID]
        message_type = fields[message_protocol.internal.TYPE]
        payload = fields[message_protocol.internal.PAYLOAD]

        if message_type == message_protocol.internal.FRUITS:
            [fruit, amount] = payload
            self._process_data(fruit, amount, client_id)

        elif message_type == message_protocol.internal.EOF:
            self._process_eof(client_id)

        else:
            logging.warning(f"Unknown message type: {message_type}")
            nack()
            return

        ack()

    def start(self):
        self.input_exchange.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()
