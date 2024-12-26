class SomeConnection:
    connection: bool = False

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 3080,
        username: str = "guest",
        password: str = "guest"
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
    
    async def connect(self):
        print("connection established")
        # Логика подключении
        self.connection = True
    
    async def send_message(self, message: str):
        if not self.connection:
            raise ValueError("Some connection is not connected to send message!")
        
        print(message)