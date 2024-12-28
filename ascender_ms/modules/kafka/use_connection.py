class UseKafkaConnection:
    def __init__(
        self,
        driver_name: str | None = None,
        producer: str | None = None,
        consumer: str | None = None
    ):
        self.driver_name = driver_name
        self.producer = producer
        self.consumer = consumer