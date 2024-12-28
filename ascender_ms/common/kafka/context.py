from dataclasses import dataclass
from typing import Any


@dataclass
class KafkaContext:
    topic: str
    partition: int
    key: bytes
    value: Any
    offset: int
    timestamp: int
    timestamp_type: int

    async def getone(self):
        ...
    
    async def position(self, pos: int):
        ...
    
    async def commit(self):
        ...