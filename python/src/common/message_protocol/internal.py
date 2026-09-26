import json

ID = "id"
TYPE = "type"
PAYLOAD = "payload"
FRUITS = "fruits"
RESULT = "result" 
EOF = "eof"

def serialize(client_id, message_type, payload):
    return json.dumps({ID: client_id, TYPE: message_type, PAYLOAD: payload}).encode("utf-8")


def deserialize(message):
    return json.loads(message.decode("utf-8"))
