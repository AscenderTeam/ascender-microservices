from typing import Literal, Sequence
from ascender.abstracts import AbstractModule, AbstractFactory

from ascender_ms.drivers.aiokafka.driver import KafkaDriver


class ProvideConnection(AbstractModule):
    def __init__(self, *connection_drivers: KafkaDriver):
        self.connection_drivers = connection_drivers
    
    def get_drivers(self, name: Literal["kafka"]):
        match name:
            case "kafka":
                return self.get_kafka_drivers()

    def get_kafka_drivers(self) -> Sequence[KafkaDriver]:
        return list(filter(lambda d: isinstance(d, KafkaDriver), self.connection_drivers))