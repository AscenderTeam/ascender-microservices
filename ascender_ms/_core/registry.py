from typing import Awaitable, Callable, Mapping, Self

from ascender_ms.common.kafka.consumer import KafkaConsumer


class ConsumerRegistry:
    _consumers: Mapping[str, list[KafkaConsumer]] = {}
    
    _instance: Self | None = None
    
    def __init__(self) -> None:
        self.scopes = None

    def __new__(cls) -> Self:
        if not cls._instance:
            cls._instance = super(ConsumerRegistry, cls).__new__(cls)
        
        return cls._instance
    
    def add_consumer(self, consumer: str, handler: KafkaConsumer):
        self._consumers[consumer] = [*self._consumers.get(consumer, []), handler]
    
    def get_consumer(self, consumer: str):
        return self._consumers.get(consumer, [])