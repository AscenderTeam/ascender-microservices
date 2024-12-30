from typing import Literal, Sequence
from ascender.abstracts import AbstractModule, AbstractFactory

from ascender_ms._core.registry import ConsumerRegistry
from ascender_ms.drivers.aiokafka.driver import KafkaDriver


class ProvideConnection(AbstractModule):
    def __init__(
        self, 
        *connection_drivers: KafkaDriver, 
        default_driver: str | None = None
    ):
        self.default_driver = default_driver
        self.connection_drivers = connection_drivers
        self.consumer_registry = ConsumerRegistry()
    
    def get_drivers(self, name: Literal["kafka"]):
        match name:
            case "kafka":
                return self.get_kafka_drivers()

    def get_kafka_drivers(self) -> Sequence[KafkaDriver]:
        return list(filter(lambda d: isinstance(d, KafkaDriver), self.connection_drivers))
    
    async def on_application_bootstrap(self, application):
        for driver in self.connection_drivers:
            await driver.connect()
        
        self.consumer_registry.on_application_bootstrap()
    
    async def on_application_shutdown(self, application):
        for driver in self.connection_drivers:
            await driver.disconnect()