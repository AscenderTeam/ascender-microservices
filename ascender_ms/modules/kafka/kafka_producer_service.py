import json
from typing import Any
from ascender.common import Injectable
from ascender.core.application import Application
from ascender.contrib.services import Service
from pydantic import BaseModel
from aiokafka import AIOKafkaProducer

from ascender_ms.exceptions.connection_not_found import ConnectionNotFound
from ascender_ms.exceptions.driver_not_found import DriverNotFound
from ascender_ms.modules.kafka.batch_builder import KafkaBatchBuilder
from ascender_ms.modules.kafka.use_connection import UseKafkaConnection
from ascender_ms.modules.provider import ProvideConnection

from aiokafka.producer.message_accumulator import BatchBuilder


@Injectable()
class KafkaProducerService(Service):
    def __init__(
        self,
        application: Application,
        connections: ProvideConnection,
        controller_connection: UseKafkaConnection = UseKafkaConnection()
    ):
        self._application = application
        self.connections = connections
        self.controller_connection = controller_connection

        self._application.app.add_event_handler("startup", self.on_application_bootstrap)
    
    def on_application_bootstrap(self):
        self._driver = next((x for x in self.connections.get_kafka_drivers(
        ) if x.driver_name == self.controller_connection.driver_name), next((x for x in self.connections.get_kafka_drivers(
        ) if x.driver_name == self.connections.default_driver), None))
        
        if not self._driver:
            raise DriverNotFound(
                "Kafka driver was either not found or connected!")

        self._producer: AIOKafkaProducer = self._driver.get_producer(
            self.controller_connection.producer) if self.controller_connection.producer else self._driver.default_producer.values()[0]

        if not self._producer:
            raise ConnectionNotFound("Kafka producer was not found")

    async def send_message(
        self,
        topic: str | int,
        value: Any | BaseModel | None = None,
        key: bytes | None = None,
        partition: int | None = None,
        timestamp_ms: Any | None = None,
        headers: Any | None = None
    ):
        """
        Sends message into topic of partition

        Args:
            topic (str): topic where the message will be published
            value (Any | BaseModel, optional): message value
                See `Kafka compaction documentation
                <https://kafka.apache.org/documentation.html#compaction>`__ for
                more details. (compaction requires kafka >= 0.8.1)
            partition (int, Optional): optionally specify a partition. If not
                set, the partition will be selected using the configured
                `partitioner`.
            key (Optional): a key to associate with the message. Can be used to
                determine which partition to send the message to. If partition
                is :data:`None` (and producer's partitioner config is left as default),
                then messages with the same key will be delivered to the same
                partition (but if key is :data:`None`, partition is chosen randomly).
                Must be type :class:`bytes`, or be serializable to bytes via configured
                `key_serializer`.
            timestamp_ms (int, Optional): epoch milliseconds (from Jan 1 1970
                UTC) to use as the message timestamp. Defaults to current time.
            headers (Optional): Kafka headers to be included in the message using
                the format ``[("key", b"value")]``. Iterable of tuples where key
                is a normal string and value is a byte string.

        Returns:
            asyncio.Future: object that will be set when message is
            processed

        Raises:
            ~aiokafka.errors.KafkaTimeoutError: if we can't schedule this record
                (pending buffer is full) in up to `request_timeout_ms`
                milliseconds.

        Note:
            The returned future will wait based on `request_timeout_ms`
            setting. Cancelling the returned future **will not** stop event
            from being sent, but cancelling the :meth:`send` coroutine itself
            **will**.
        """
        value = self.__convert_data(value)
        return await self._producer.send(topic, value, key, partition, timestamp_ms, headers)

    async def send_with_ack(
        self,
        topic: str | int,
        value: Any | BaseModel | None = None,
        key: bytes | None = None,
        partition: Any | None = None,
        timestamp_ms: Any | None = None,
        headers: Any | None = None
    ):
        """Sends message into partition topic and waits for result (acknowledgement)"""
        value = self.__convert_data(value)
        return await self._producer.send_and_wait(topic, value, key, partition, timestamp_ms, headers)

    async def send_batch(
        self,
        batch_builder: KafkaBatchBuilder,
        topic: str | int,
        *,
        partition: int
    ):
        """
        Submits a [KafkaBatchBuilder](./batch_builder.py) for publication.

        Args:
            batch_builder (KafkaBatchBuilder): `KafkaBatchBuilder` object being submit for publication
            topic (str | int): topic where the message will be published
            partition (int): specify a partition where to publish the batch builder

        Returns:
            asyncio.Future: An asynchronouse Future task which can be used for acknowledgement
        """
        return await self._producer.send_batch(batch_builder, topic, partition=partition)
    
    def initiate_batch(self):
        """
        Create and return an empty .`KafkaBatchBuilder`.

        The batch is not queued for send until submission to send_batch.

        Returns:
            KafkaBatchBuilder: empty batch to be filled and submitted by the caller.
        """
        return self._producer.create_batch()
    
    @property
    def transaction(self):
        return self._producer.transaction()
    
    @property
    def raw_producer(self):
        return self._producer

    def __convert_data(self, value: Any | BaseModel | None = None):
        if isinstance(value, BaseModel):
            value = value.model_dump_json().encode()
        
        elif isinstance(value, (dict, list, bool)):
            value = json.dumps(value).encode()
        
        elif isinstance(value, (str, int, float)):
            value = value.encode()

        return value