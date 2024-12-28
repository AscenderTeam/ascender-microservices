from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from ascender.common.api_docs import DefineAPIDocs
from ascender.core.database.provider import DatabaseProvider
from ascender.core.database.types.orm_enum import ORMEnum
from ascender.core.types import IBootstrap
from ascender.core.utils.controller_module import ProvideControllers
from ascender_ms.drivers.aiokafka.driver import KafkaDriver
from controllers.controllers_module import ControllersModule
from settings import DATABASE_CONNECTION

from ascender_ms.modules.provider import ProvideConnection


appBootstrap: IBootstrap = {
    "providers": [
        DefineAPIDocs(title="Ascender Framework API", swagger_url="/docs", redoc_url="/redoc"),
        ProvideConnection(
            KafkaDriver(
                driver_name="kafka_1",
                producers={"producer_1": lambda: AIOKafkaProducer()},
                consumers={"consumer_1": lambda: AIOKafkaConsumer("test")},
                default_producer="producer_1",
                default_consumer="consumer_1"
            ),
            default_driver="kafka_1"
        ),
        DatabaseProvider(ORMEnum.SQLALCHEMY, DATABASE_CONNECTION),
        ProvideControllers([
            ControllersModule
        ]),
    ]
}