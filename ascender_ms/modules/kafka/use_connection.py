class UseKafkaConnection:
    def __init__(
        self,
        driver_name: str,
        producer: str | None = None,
        consumer: str | None = None
    ):
        self.driver_name = driver_name
        self.producer = producer
        self.consumer = consumer