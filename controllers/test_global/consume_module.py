from ascender.common.module import AscModule

from ascender_ms.modules.kafka.kafka_global_service import KafkaGlobalService
from controllers.test_global.consume_controller import ConsumeController


@AscModule(
    imports=[],
    declarations=[ConsumeController],
    providers=[KafkaGlobalService],
    exports=[]
)
class ConsumeGlobalModule:
    ...