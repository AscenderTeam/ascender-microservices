from abc import abstractmethod


class AbstractAIODriver:
    
    @abstractmethod
    async def send_message(self):
        ...

    async def send_with_ack(self):
        ...