from typing import Mapping
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer


class KafkaDriver:
    def __init__(
        self,
        driver_name: str,
        producers: Mapping[str, AIOKafkaProducer] = {},
        consumers: Mapping[str, AIOKafkaConsumer] = {},
        default_producer: str | None = None,
        default_consumer: str | None = None
    ):
        self.driver_name = driver_name
        self.producers = producers
        self.consumers = consumers
        self.default_producer = {default_producer: self.producers[default_producer]}
        self.default_consumer = {default_consumer: self.consumers[default_consumer]}
    
    async def connect(self):
        """
        Initializes and starts connection of all producers and consumers.
        Returns nothing but runs all connections one by one.
        """
        for producer in self.producers.values():
            await producer.start()
        
        for consumer in self.consumers.values():
            await consumer.start()
        
    async def disconnect(self):
        """
        Deinitializes connections of producers & consumers.
        Returns nothing but stops all connections one by one.
        """
        for producer in self.producers.values():
            await producer.stop()
        
        for consumer in self.consumers.values():
            await consumer.stop()
    
    def get_producer(self, producer: str):
        """
        Gets producer and returns it's connection object

        Args:
            producer (str): Name of producer connection

        Returns:
            AIOKafkaProducer: Kafka producer object
            None: Nothing if it wasn't found
        """
        return self.producers.get(producer)
    
    def get_consumer(self, consumer: str):
        """
        Gets consumer and returns it's connection object

        Args:
            consumer (str): Name of consumer

        Returns:
            AIOKafkaConsumer: Kafka consumer object
            None: Nothing if it wasn't found
        """
        return self.consumers.get(consumer)