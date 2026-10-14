import json
from unittest.mock import MagicMock

from tasks.broadcast.send_status_messages import _send_status_message


def test_hal_collection_is_broadcast_in_structures_event(monkeypatch):
    """The combined record, including hal_collection, is sent as is in structures_event.data."""
    monkeypatch.setenv("AMQP_STRUCTURES_MESSAGE_PREFIX", "event.structures.structure.")
    channel = MagicMock()
    data = {
        "generic_type": "unit",
        "type": "UMR",
        "identifiers": [{"type": "local", "value": "U1"}],
        "hal_collection": "https://hal.science/COLLECTION",
    }

    params = _send_status_message(channel, {"status": "created", "data": data}, "structures")

    channel.basic_publish.assert_called_once_with(**params)
    body = json.loads(params["body"])
    assert body["structures_event"]["type"] == "created"
    assert body["structures_event"]["data"] == data
    assert body["structures_event"]["data"]["hal_collection"] == "https://hal.science/COLLECTION"
