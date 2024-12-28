from typing import Callable, Mapping
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer


class KafkaDriver:
    def __init__(
        self,
        driver_name: str,
        producers: Mapping[str, Callable[[], AIOKafkaProducer]] = {},
        consumers: Mapping[str, Callable[[], AIOKafkaConsumer]] = {},
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
        print("Initializing kafka")
        for key, producer in self.producers.items():
            self.producers[key] = producer()
            await self.producers[key].start()
            print(key, self.producers[key])
        
        for key, consumer in self.consumers.items():
            self.consumers[key] = consumer()
            await self.consumers[key].start()
        
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