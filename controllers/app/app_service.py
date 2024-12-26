from ascender.common import Injectable
from ascender.contrib.services import Service

from examples.connection import SomeConnection


@Injectable()
class AppService(Service):
    some_connection: SomeConnection
    
    def __init__(self):
        ...

    async def send_message(self, msg: str):
        await self.some_connection.send_message(msg)