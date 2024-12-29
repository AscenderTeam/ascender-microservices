import inspect
import json
from typing import Any, Callable

from pydantic import BaseModel
from ascender_ms._core.registry import ConsumerRegistry
from ascender_ms.common.kafka.context import KafkaContext


class KafkaConsumer:
    def __init__(
        self, 
        driver: str | None = None,
        connection: str | None = None,
        *,
        topic: str | None = None,
        key: bytes | None = None,
        partition: int | None = None
    ):
        self.driver = driver
        self.connection = connection
        self.topic = topic
        self.key = key
        self.partition = partition

        self.consumer_registry = ConsumerRegistry()
        self.consumer_registry.add_consumer("kafka", self)

    async def handle_callback(
        self, 
        context: KafkaContext,
        executable: Callable[[KafkaContext, Any], None] | Callable[[KafkaContext], None]
    ):
        await executable()
    
    def __executable_sig(self, value: bytes | None, executable: Callable[[KafkaContext, Any], None] | Callable[[KafkaContext], None]):
        _signature = inspect.signature(executable)

        payload = {}
        for name, param in _signature.parameters.items():
            if value is None and param.default == inspect.Parameter.empty:
                raise ValueError(f"Consumer requires value `${param.annotation}` but nothing received")
            
            if issubclass(param.annotation, BaseModel):
                    payload[name] = param.annotation.model_validate_json(value.decode())
            
            else:
                try:
                    payload[name] = json.loads(value.decode())
                except:
                    payload[name] = value.decode()
        
        return payload    

    def __convert(self, executable: Callable, value: bytes):
        return

    def __call__(self, executable: Callable[[KafkaContext, Any], None] | Callable[[KafkaContext], None]):
        return executable