import json
from typing import Any, Sequence
from aiokafka.producer.message_accumulator import BatchBuilder
from pydantic import BaseModel


class KafkaBatchBuilder:
    def __init__(self, batch_builder: BatchBuilder):
        self.batch_builder = batch_builder

    def append(
        self,
        *,
        timestamp: float | None = None,
        key: bytes | None = None,
        value: Any | BaseModel | None = None,
        headers: Sequence = []
    ):
        """
        Add a message to the batch.

        Args:
            timestamp (float or None): epoch timestamp in seconds. If None,
                the timestamp will be set to the current time. If submitting to an 0.8.x or 0.9.x broker, the timestamp will be ignored.
            key (bytes or None): the message key. `key` and `value` may not
                both be `None`.
            value (bytes or None): the message value. `key` and `value` may not
                both be `None`.

        Returns:
            If the message was successfully added, returns a metadata object with crc, offset, size, and timestamp fields. If the batch is full or closed, returns None.
        """
        value = self.__convert_data(value)
        return self.batch_builder.append(
            timestamp=timestamp,
            key=key, value=value,
            headers=headers
        )
    
    def close(self) -> None:
        """
        Closes the batch for further updates
        """
        return self.batch_builder.close()
    
    @property
    def size(self) -> int:
        """
        Get the size of batch in bytes.
        """
        return self.batch_builder.size()
    
    @property
    def record_count(self) -> int:
        """
        Get the number of records in the batch.
        """
        return self.batch_builder.record_count()

    def __convert_data(self, value: Any | BaseModel | None = None):
        if isinstance(value, BaseModel):
            value = value.model_dump_json().encode()

        elif isinstance(value, (dict, list, bool)):
            value = json.dumps(value).encode()

        elif isinstance(value, (str, int, float)):
            value = value.encode()

        return value
