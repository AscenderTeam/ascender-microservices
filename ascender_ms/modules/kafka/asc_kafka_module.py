from ascender_ms.modules.kafka.kafka_producer_service import KafkaProducerService
from ascender.common.module import AscModule


@AscModule(
    imports=[
    ],
    declarations=[
    ],
    providers=[
        KafkaProducerService,
    ],
    exports=[]
)
class AscKafkaModule:
    ...