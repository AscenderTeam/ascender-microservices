from __future__ import annotations
from typing import TYPE_CHECKING, Self

if TYPE_CHECKING:
    from ascender_ms.common.kafka.consumer import KafkaConsumer


class ConsumerRegistry:
    _consumers: list[KafkaConsumer] = []
    
    _instance: Self | None = None
    
    def __init__(self) -> None:
        self.scopes = None

    def __new__(cls) -> Self:
        if not cls._instance:
            cls._instance = super(ConsumerRegistry, cls).__new__(cls)
        
        return cls._instance
    
    def add_consumer(self, consumer: KafkaConsumer):
        self._consumers.append(consumer)
    
    def on_application_bootstrap(self):
        for consumer in self._consumers:
            consumer.handle_di()