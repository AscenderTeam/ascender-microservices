from ascender.common.api_docs import DefineAPIDocs
from ascender.core.database.provider import DatabaseProvider
from ascender.core.database.types.orm_enum import ORMEnum
from ascender.core.types import IBootstrap
from ascender.core.utils.controller_module import ProvideControllers
from controllers.controllers_module import ControllersModule
from settings import DATABASE_CONNECTION


appBootstrap: IBootstrap = {
    "providers": [
        DefineAPIDocs(title="Ascender Framework API", swagger_url="/docs", redoc_url="/redoc"),
        DatabaseProvider(ORMEnum.SQLALCHEMY, DATABASE_CONNECTION),
        ProvideControllers([
            ControllersModule
        ]),
    ]
}