from examples.connection import SomeConnection
from ascender.abstracts import AbstractModule, AbstractFactory


class ProvideConnection(AbstractModule, AbstractFactory):
    
    def __init__(self, host: str, port: int, username: str, password: str):
        self.connection = SomeConnection(host, port, username, password)
    
    async def on_application_bootstrap(self, application):
        await self.connection.connect()
        application.service_registry.add_singletone(SomeConnection, self.connection)