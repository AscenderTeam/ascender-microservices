from ascender.core.utils.controller import Controller, Get, Post

from ascender_ms.common.kafka.consumer import KafkaConsumer
from ascender_ms.common.kafka.context import KafkaContext
from ascender_ms.modules.kafka.kafka_producer_service import KafkaProducerService
from ascender_ms.modules.kafka.kafka_consumer_service import KafkaConsumerService

from functools import partial


@Controller(
    standalone=False,
    guards=[],
)
class ConsumeController:
    def __init__(self, kafka_producer: KafkaProducerService, kafka_consumer: KafkaConsumerService):
        self.kafka_producer = kafka_producer
        self.kafka_consumer = kafka_consumer

    @Post("/test_topic/send_custom_message")
    async def send_custom_message(self, message: str, partition: int, key: str):
        """
        Sends a custom message to the "test_topic" with specified partition and key.

        Parameters:
            message: str
                The message to be sent.
            partition: int
                The partition number where the message will be sent.
            key: str
                The key for the message, which will be encoded to bytes.
        
        Returns:
            str: "ready" once the message is sent.
        """

        await self.kafka_producer.send_message(topic="test_topic", value=message, key=key.encode('utf-8'), partition=partition)
        return "ready"
    


    @Post("/test/send_json")
    async def send_json(self):
        """
        Sends a JSON message to the "test" Kafka topic with predefined data.

        Returns:
            str: "ready" once the message is sent.
        """

        await self.kafka_producer.send_message(topic="test", value={"name": "Gosha", "YO": "twelwe"})
        return "ready"
    

    # @KafkaConsumer(topic="test")
    async def get_json(self, ctx: KafkaContext):
        """
        Subscribes to the "test" topic for consuming messages and uses a handler to process the messages.

        Returns:
            None
        """
        print(f"value: {ctx.value}   key: {ctx.key}   partition: {ctx.partition}")
    

    async def my_handler(self, ctx: KafkaContext):
        """
        Handler function for processing messages from Kafka.

        Parameters:
            ctx: KafkaContext
                The context object containing the message details.
        
        Prints the message value, key, and partition.
        """

        print(f"value: {ctx.value}   key: {ctx.key}   partition: {ctx.partition}")


    @Get("/test_topic/all")
    async def test_topic_all(self):
        """
        Subscribes to all messages from the "test_topic" topic.

        Returns:
            None
        """
    
        await self.kafka_consumer.subscribe(topic="test_topic", key=None, partition=None, handler=partial(self.my_handler))


    @Get("/test_topic/partition")
    async def test_topic_partition(self):
        """
        Subscribes to the "test_topic" topic for messages from partition 1 only.

        Returns:
            None
        """

        await self.kafka_consumer.subscribe(topic="test_topic", key=None, partition=1, handler=partial(self.my_handler))


    @Get("/test_topic/key")
    async def test_topic_key(self):
        """
        Subscribes to the "test_topic" topic for messages with key "work".

        Returns:
            None
        """

        await self.kafka_consumer.subscribe(topic="test_topic", key=b"work", partition=None, handler=partial(self.my_handler))
    

    @Get("/test_topic/partition_and_key")
    async def test_topic_partition_and_key(self):
        """
        Subscribes to the "test_topic" topic for messages with key "work" and partition 1.

        Returns:
            None
        """
         
        await self.kafka_consumer.subscribe(topic="test_topic", key=b"work", partition=1, handler=partial(self.my_handler))
    

    @Get("/all")
    async def test_all(self):
        """
        Subscribes to all messages from all topics and partitions.

        Returns:
            None
        """

        await self.kafka_consumer.subscribe(topic=None, key=None, partition=None, handler=partial(self.my_handler))
    


    @Get("/unsubscribe")
    async def unsubscribe(self):
        """
        Unsubscribes from the topic with the identifier "All".

        Returns:
            None
        """
         
        await self.kafka_consumer.unsubscribe("All")


    @Get("/unsubscribe_from_all")
    async def unsubscribe_from_all(self):
        """
        Unsubscribes from all active subscriptions.

        Returns:
            None
        """
         
        await self.kafka_consumer.unsubscribe_from_all()



    @Get("/get_all_subscriptions")
    async def get_all_subscriptions(self):
        """
        Retrieves a list of all current subscriptions.

        Returns:
            list
                A list of active subscriptions.
        """
        return await self.kafka_consumer.get_all_subscriptions()