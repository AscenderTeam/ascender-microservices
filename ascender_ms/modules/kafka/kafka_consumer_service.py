from typing import Callable, Any, Coroutine
from aiokafka import ConsumerRecord, AIOKafkaConsumer
from ascender.common import Injectable
from ascender.contrib.services import Service
from ascender.core.application import Application

from ascender_ms.common.kafka.context import KafkaContext
from ascender_ms.modules.provider import ProvideConnection
from ascender_ms.modules.kafka.use_connection import UseKafkaConnection

from ascender_ms.exceptions.driver_not_found import DriverNotFound
from ascender_ms.exceptions.connection_not_found import ConnectionNotFound

import asyncio
import json


@Injectable()
class KafkaConsumerService(Service):
    """
    Service for interacting with Kafka Consumer, allowing subscription to topics,
    unsubscribing, and processing messages received from Kafka.

    Attributes:
        _application: Application
            The application the service is associated with.
        connections: ProvideConnection
            Object for fetching Kafka connection details.
        controller_connection: UseKafkaConnection
            Kafka connection details used to initialize the consumer.
        driver_name: str | None
            Kafka driver name used for the connection.
        consumer_name: str | None
            Kafka consumer name used for the connection.
        _consumers: dict
            A dictionary holding active subscriptions and their respective tasks.
        _consumer: AIOKafkaConsumer
            Kafka consumer instance used to fetch messages.
    """

    def __init__(
            self,
            application: Application,
            connections: ProvideConnection,
            controller_connection: UseKafkaConnection = UseKafkaConnection(),
            driver_name: str | None = None,
            consumer_name: str | None = None
        ):
        """
        Initializes the KafkaConsumerService.

        Parameters:
            application: Application
                The application the service is associated with.
            connections: ProvideConnection
                Object to fetch Kafka connection details.
            controller_connection: UseKafkaConnection
                Kafka connection details used for consumer initialization.
            driver_name: str | None
                Kafka driver name (defaults to value from controller_connection).
            consumer_name: str | None
                Kafka consumer name (defaults to value from controller_connection).
        """

        self._application = application
        self.connections = connections
        self.controller_connection = controller_connection

        self.driver_name = driver_name or self.controller_connection.driver_name
        self.consumer_name = consumer_name or self.controller_connection.consumer
        self._consumers = {}

        self._application.app.add_event_handler("startup", self.on_application_bootstrap)


    def on_application_bootstrap(self):
        """
        Initializes the Kafka connection when the application starts. It finds the appropriate Kafka driver
        and sets up the consumer for use by the service.

        Raises:
            DriverNotFound: If no suitable Kafka driver or connection is found.
            ConnectionNotFound: If the Kafka consumer is not found.
        """

        self._driver = next((x for x in self.connections.get_kafka_drivers(
        ) if x.driver_name == self.controller_connection.driver_name), next((x for x in self.connections.get_kafka_drivers(
        ) if x.driver_name == self.connections.default_driver), None))

        if not self._driver:
            raise DriverNotFound(
                "Kafka was either not found or connected!")
        
        self._consumer: AIOKafkaConsumer = self._driver.get_consumer(
            self.controller_connection.consumer) if self.controller_connection.consumer else self._driver.default_consumer.values()[0]
        
        if not self._consumer:
            raise ConnectionNotFound("Kafka consumer was not found")


    async def subscribe(self, topic: str | None, key: bytes | None, partition: int | None, handler: Callable[[KafkaContext], Coroutine[Any, Any, None]]):
        """
        Subscribes to a Kafka topic with optional filtering by key and partition,
        and processes the received messages using a handler function.

        Parameters:
            topic: str | None
                The topic to subscribe to.
            key: bytes | None
                The key to filter messages by (if None, no key filtering is applied).
            partition: int | None
                The partition of the topic to subscribe to (if None, subscribes to all partitions).
            handler: Callable[[KafkaContext], Coroutine[Any, Any, None]]
                A handler function that will be called for each received message.
        
        Raises:
            ValueError: If already subscribed to the specified topic/partition/key.
        """
        
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

