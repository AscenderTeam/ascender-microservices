from ascender.core.utils.controller import Controller, Get, Post

from ascender_ms.common.kafka.consumer import KafkaConsumer
from ascender_ms.common.kafka.context import KafkaContext
from ascender_ms.modules.kafka.kafka_producer_service import KafkaProducerService
from ascender_ms.modules.kafka.kafka_global_service import KafkaGlobalService

from functools import partial


@Controller(
    standalone=False,
    guards=[],
)
class ConsumeController:
    def __init__(self, kafka_consumer: KafkaGlobalService):
        self.kafka_consumer = kafka_consumer

    async def my_handler(self, ctx: KafkaContext):
        print(f"value: {ctx.value}   key: {ctx.key}   partition: {ctx.partition}")
    
    
    @Get("GLOBAL/test_topic/all")
    async def test_topic_all(self):
        await self.kafka_consumer.subscribe(driver="kafka_1", connection="consumer_1",
                                             topic="test_topic", key=None, partition=None, handler=partial(self.my_handler))