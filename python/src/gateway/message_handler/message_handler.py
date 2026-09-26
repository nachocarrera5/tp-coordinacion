import uuid

from common import message_protocol


class MessageHandler:

    def __init__(self):
        self._id = uuid.uuid4().hex
    
    def serialize_data_message(self, message):
        [fruit, amount] = message
        return message_protocol.internal.serialize(self._id, message_protocol.internal.FRUITS, [fruit, amount])

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize(self._id, message_protocol.internal.EOF, None)

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)

        if fields[message_protocol.internal.ID] != self._id:
            return None

        if fields[message_protocol.internal.TYPE] != message_protocol.internal.RESULT:
            return None
        
        return fields[message_protocol.internal.PAYLOAD]
