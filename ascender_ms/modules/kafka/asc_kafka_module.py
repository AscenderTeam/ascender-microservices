from ascender_ms.modules.kafka.kafka_consumer_service import KafkaConsumerService
from ascender_ms.modules.kafka.kafka_producer_service import KafkaProducerService
from ascender.common.module import AscModule


@AscModule(
    imports=[
    ],
    declarations=[
    ],
    providers=[
        KafkaConsumerService,
        KafkaProducerService,
    ],
    exports=[]
)
class AscKafkaModule:
    ...