from typing import Callable
from aiokafka import ConsumerRecord
from ascender.common import Injectable
from ascender.contrib.services import Service

from ascender_ms.common.kafka.context import KafkaContext


@Injectable()
class KafkaConsumerService(Service):
    def __init__(self):
        ...
    
    async def subscribe(self, callback: Callable[[KafkaContext], None]):
        async for msg in self.consumer:
            try:
                await callback(self.create_context(msg), msg.value)
            except:
                """
                Handle exception somehow
                """
    
    def create_context(self, msg: ConsumerRecord):
        return KafkaContext(msg.topic, msg.partition)