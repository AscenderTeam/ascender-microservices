from typing import Callable, Any, Coroutine
from aiokafka import ConsumerRecord, AIOKafkaConsumer
from ascender.common import Injectable
from ascender.contrib.services import Service
from ascender.core.application import Application

from ascender_ms.common.kafka.context import KafkaContext
from ascender_ms.drivers.aiokafka.driver import KafkaDriver
from ascender_ms.modules.provider import ProvideConnection
from ascender_ms.modules.kafka.use_connection import UseKafkaConnection

from ascender_ms.exceptions.driver_not_found import DriverNotFound
from ascender_ms.exceptions.connection_not_found import ConnectionNotFound

import asyncio
import json



@Injectable()
class KafkaGlobalService(Service):
    def __init__(
            self,
            application: Application,
            connections: ProvideConnection,
        ):
        self._application = application
        self.connections = connections
        self._consumers = {}

        self._application.app.add_event_handler("startup", self.on_application_bootstrap)


    def on_application_bootstrap(self):
        self._drivers = self.connections.get_kafka_drivers()


    async def subscribe(
                    self,
                    driver: str | None,
                    connection: str | None,
                    topic: str | None, 
                    key: bytes | None, 
                    partition: int | None, 
                    handler: Callable[[KafkaContext], Coroutine[Any, Any, None]]):
        """
        Subscribes to a Kafka topic using the specified driver and connection.

        Args:
            driver (str | None): The name of the Kafka driver to use. If None, the default driver will be used.
            connection (str | None): The name of the consumer connection. If None, the default consumer will be used.
            topic (str | None): The Kafka topic to subscribe to. If None, messages from all topics will be consumed.
            key (bytes | None): The key of the messages to filter by. If None, all keys will be accepted.
            partition (int | None): The partition of the topic to filter by. If None, all partitions will be accepted.
            handler (Callable[[KafkaContext], Coroutine[Any, Any, None]]):
                An asynchronous function that will handle each consumed message. It receives a `KafkaContext` object.

        Raises:
            ConnectionNotFound: If the specified or default Kafka consumer connection cannot be found.
            ValueError: If already subscribed to the specified topic, key, and partition.

        Usage Example:
            async def message_handler(context: KafkaContext):
                print(f"Received message: {context.value}")

            await service.subscribe(
                driver="my_driver",
                connection="my_connection",
                topic="my_topic",
                key=None,
                partition=0,
                handler=message_handler
            )
        """
        
        if driver is None: self._driver: KafkaDriver = self.connections.find_driver_by_name(self.connections.default_driver)
        else: self._driver: KafkaDriver = self.connections.find_driver_by_name(driver)

        self._consumer: AIOKafkaConsumer = self._driver.get_consumer(connection)\
            if connection else self._driver.get_consumer(list(self._driver.default_consumer.keys())[0])
        
        if not self._consumer:
            raise ConnectionNotFound("Kafka consumer was not found")
        
        print(self._consumer)

        subscription_id = (topic, key, partition)
        if subscription_id in self._consumers:
            raise ValueError(f"Already subsribed to topic: {topic}, partition: {partition}, key: {key}")
        
        async def consume():
            async for message in self._consumer:
                if topic is not None and message.topic != topic:
                    continue

                if partition is not None and message.partition != partition:
                    continue

                if key is not None and message.key != key:
                    continue
                
                parsed_value = await self.__parse_message(message.value)
                context = await self.create_context(message, parsed_value)
                await handler(context)

        consume_task = asyncio.create_task(consume())

        if subscription_id == (None, None, None): self._consumers["all_topics"] = consume_task
        else: self._consumers[subscription_id] = consume_task



    async def unsubscribe(self, topic: str | None, key: bytes | None, partition: int | None):
        """
        Unsubscribes from a specific topic, key, and partition.

        Parameters:
            topic: str | None
                The topic to unsubscribe from.
            key: bytes | None
                The key to unsubscribe from (if None, no key filtering is applied).
            partition: int | None
                The partition to unsubscribe from (if None, no partition filtering is applied).
        
        Raises:
            ValueError: If there is no active subscription for the specified topic.
        """

        subscription_id = (topic, key, partition) if topic is not "all_topics" else topic
        if not subscription_id in self._consumers:
            raise ValueError(f"You are not subsribed to topic: {topic}")

        consume_task = self._consumers.pop(subscription_id)
        consume_task.cancel()
    

    async def unsubscribe_from_all(self):
        """
        Unsubscribes from all active subscriptions and clears the subscription dictionary.
        """

        for subscription_id, consume_task in self._consumers.items():
            consume_task.cancel()
        
        self._consumers.clear()
        return


    async def get_all_subscriptions(self):
        """
        Returns a list of all active subscriptions.

        Returns:
            list
                A list of all subscriptions represented as tuples (topic, key, partition).
        """

        return list(self._consumers.keys())


    async def create_context(self, msg: ConsumerRecord, parsed_value: Any) -> KafkaContext:
        """
        Creates a KafkaContext object to process a message.

        Parameters:
            msg: ConsumerRecord
                The Kafka message received.
            parsed_value: Any
                The parsed value of the message.

        Returns:
            KafkaContext
                The KafkaContext object containing the message details and parsed value.
        """

        return KafkaContext(
            topic=msg.topic,
            partition=msg.partition,
            key=msg.key,
            value=parsed_value,
            offset=msg.offset,
            timestamp=msg.timestamp,
            timestamp_type=msg.timestamp_type
        )
    

    
    async def __parse_message(self, value: bytes):
        """
        Converts the byte value of the message to a Python type (string or JSON object).

        Parameters:
            value: bytes
                The byte representation of the message value.

        Returns:
            str | dict
                The converted value as either a string or a JSON object.
        """
         
        try:
            return json.loads(value.decode('utf-8'))
        except (json.JSONDecodeError, AttributeError):
            try:
                return value.decode('utf-8')
            except (UnicodeDecodeError, AttributeError):
                return value