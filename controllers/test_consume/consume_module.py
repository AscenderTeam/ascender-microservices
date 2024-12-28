from ascender.common.module import AscModule

from controllers.test_consume.consume_controller import ConsumeController
from ascender_ms.modules.kafka.asc_kafka_module import AscKafkaModule
from ascender_ms.modules.kafka.use_connection import UseKafkaConnection


@AscModule(
    imports=[AscKafkaModule],
    declarations=[ConsumeController],
    providers=[UseKafkaConnection(driver_name="kafka_1", producer="producer_1", consumer="consumer_1")],
    exports=[]
)
class ConsumeModule:
    ...