import inspect
import json
from typing import Any, Callable, Mapping

from pydantic import BaseModel
from ascender.abstracts import AbstractModule
from ascender_ms.common.kafka.context import KafkaContext

from ascender_ms.modules.kafka.kafka_consumer_service import KafkaConsumerService
from ascender_ms.validation.strategies.general import GeneralValidationStrategy
from ascender_ms.validation.strategies.jsonv import JSONValidationStrategy
from ascender_ms.validation.validation import Validator


class KafkaConsumer(AbstractModule):
    """
    A consumer decorator appliable for controller.
    Subscribes to Kafka Consumer's events and pattern described in arguments and parameters of this decorator

    Usage:
    ```py
    @Controller(standalone=False)
    class MyController:
        kafka_consumer: KafkaConsumer

        @kafka_consumer(topic="1", key=b"2")
        async def handle_first_consumer(self, ctx: KafkaContext):
            await ctx.commit()
    ```
    """
    
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

        self.__callback_executable = None

    async def handle_callback(
        self, 
        context: KafkaContext
    ):
        """
        This callback used for handling incoming messages, converting and reverbating it to the callback function of controller
        """
        payload = self.__executable_sig(context, context.value, self.__callback_executable) # type: ignore
        
        await self.__callback_executable(**payload) # type: ignore
    
    def __executable_sig(
        self, 
        ctx: KafkaContext,
        value: bytes | None, 
        executable: Callable[[KafkaContext, Any], None] | Callable[[KafkaContext], None]
    ) -> Mapping[str, KafkaContext | BaseModel | Any]:
        
        _signature = inspect.signature(executable)

        payload: Mapping[str, KafkaContext | BaseModel | Any] = {}
        for name, param in _signature.parameters.items():
            if value is None and param.default == inspect.Parameter.empty:
                raise ValueError(f"Consumer requires value `${param.annotation}` but nothing received")
            
            if issubclass(param.annotation, KafkaContext) or name == "ctx":
                payload[name] = ctx
                continue
            
            if isinstance(value, bytes):
                value = value.decode()

            if issubclass(param.annotation, BaseModel):
                payload[name] = Validator(JSONValidationStrategy(5)).validate(param, value)
            else:
                payload[name] = Validator(GeneralValidationStrategy()).validate(param, value)
        
        return payload

    def can_activate(self):
        return super().can_activate()

    def __call__(self, executable: Callable[[KafkaContext, Any], None] | Callable[[KafkaContext], None]):
        self.__callback_executable = executable
        
        self.registry.add_consumer(self)
        return executable