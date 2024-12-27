from typing import Any
from ascender.common import Injectable
from ascender.contrib.services import Service
from pydantic import BaseModel

from ascender_ms.exceptions.connection_not_found import ConnectionNotFound
from ascender_ms.exceptions.driver_not_found import DriverNotFound
from ascender_ms.modules.kafka.use_connection import UseKafkaConnection
from ascender_ms.modules.provider import ProvideConnection


@Injectable()
class KafkaProducerService(Service):
    def __init__(
        self,
        connections: ProvideConnection,
        controller_connection: UseKafkaConnection = UseKafkaConnection()
    ):
        self.connections = connections
        self.controller_connection = controller_connection

        self._driver = next((x for x in self.connections.get_kafka_drivers(
        ) if x.driver_name == controller_connection.driver_name), None)
        if not self._driver:
            raise DriverNotFound(
                "Kafka driver was either not found or connected!")

        self._producer = self._driver.get_producer(
            controller_connection.producer) if controller_connection.producer else self._driver.default_producer.values()[0]

        if not self._producer:
            raise ConnectionNotFound("Kafka producer was not found")

    async def send_message(
            self,
            topic: str,
            value: Any | BaseModel | None = None
    ):
        return self._producer.send(topic, )
