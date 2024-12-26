from ascender.core.utils.controller import Controller, Get
from ascender.common import inject
from controllers.app.app_service import AppService
from examples.connection import SomeConnection


@Controller(
    standalone=False,
    guards=[],
)
class AppController:
    
    def __init__(self, app_service: AppService):
        self.app_service = app_service
    
    @Get()
    async def send_message(self, msg: str):
        await self.app_service.send_message(msg)
        return True