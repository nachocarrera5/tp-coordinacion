import os
import logging

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

        self.top_candidates_by_client = {}
        self.results_by_client = {}

    def _process_result(self, client_id, aggregation_fruits):

        candidates = self.top_candidates_by_client.setdefault(client_id, [])

        for fruit, amount in aggregation_fruits:
            candidates.append(fruit_item.FruitItem(fruit, amount))

        results_amount = self.results_by_client.get(client_id, 0) + 1

        self.results_by_client[client_id] = results_amount

        logging.info("Received partial result")

        if results_amount < AGGREGATION_AMOUNT:
            return

        top_fruits = sorted(candidates, reverse=True)[:TOP_SIZE] # como maximo ordeno cant de aggregation x el top size

        final_result = [[item.fruit, item.amount] for item in top_fruits]

        self.output_queue.send(message_protocol.internal.serialize(client_id, message_protocol.internal.RESULT, final_result))

        self.top_candidates_by_client.pop(client_id, None)
        self.results_by_client.pop(client_id, None)

    def process_messsage(self, message, ack, nack):

        logging.info("Received top")

        message_fields = message_protocol.internal.deserialize(message)

        client_id = message_fields[message_protocol.internal.ID]
        message_type = message_fields[message_protocol.internal.TYPE]
        payload = message_fields[message_protocol.internal.PAYLOAD]

        if message_type != message_protocol.internal.RESULT:
            logging.error("Received non-result message")
            nack()
            return

        self._process_result(client_id, payload)
        ack()


    def start(self):
        self.input_queue.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()
