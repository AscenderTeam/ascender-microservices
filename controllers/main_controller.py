from ascender.core.utils.controller import Controller, Get

from ascender_ms.common.kafka.consume import Consume
from ascender_ms.modules.kafka.asc_kafka_module import AscKafkaModule
from ascender_ms.modules.kafka.kafka_producer_service import KafkaProducerService
from ascender_ms.modules.kafka.use_connection import UseKafkaConnection


@Controller(
    standalone=True,
    guards=[],
    imports=[
        AscKafkaModule
    ],
    providers=[
        UseKafkaConnection(driver_name="kafka_1", producer="producer_1")
    ],
)
class MainController:
    
    def __init__(self, kafka_producer: KafkaProducerService):
        self.kafka_producer = kafka_producer

    @Get()
    async def main_endpoint(self):
        await self.kafka_producer.send_message(topic="test", value="Test", key=b"1")
        return "main works!"

    @Consume("test")
    async def handle_test(self, ctx: KafkaContext):
        pass