from ascender_ms.common.kafka.consumer import KafkaConsumer
from ascender_ms.modules.kafka.kafka_consumer_service import KafkaConsumerService
from ascender_ms.modules.kafka.kafka_producer_service import KafkaProducerService
from ascender.common.module import AscModule

from ascender.core.di.provider import Provider

@AscModule(
    imports=[],
    declarations=[],
    providers=[
        KafkaProducerService,
        KafkaConsumerService,
    ],
    exports=[]
)
class AscKafkaModule:
    ...