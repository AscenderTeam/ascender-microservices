from dataclasses import dataclass


@dataclass
class KafkaContext:
    topic: str
    partition: int
    value: bytes
    offset: int
    timestamp: int
    timestamp_type: int

    async def getone(self):
        ...
    
    async def position(self, pos: int):
        ...
    
    async def commit(self):
        ...