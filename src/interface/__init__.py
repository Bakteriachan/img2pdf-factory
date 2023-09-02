import logging
import json

from pika import (
    BlockingConnection,
    URLParameters,
)

logging.basicConfig(
    level = logging.INFO,
    format = '[%(levelname)s] [%(name)s] [%(asctime)s] %(message)s',
)
class RabbitMQInterface:
    connection = None
    channel = None
    queues = None
    consumers = None

    @staticmethod
    def onMessageReceived(func):
        def callback(channel, method, properties, body):
            logging.info(f'[+++] Message received for consumer [{func.__name__}]')
            try:
                message = json.loads(body)
                func(message, channel)
            except Exception as e:
                print(e)
        return callback


    @staticmethod
    def connect(url):
        if not RabbitMQInterface.connection:
            RabbitMQInterface.connection = RabbitMQInterface.createConnection(url)
            RabbitMQInterface.channel = RabbitMQInterface.connection.channel()

        connection = RabbitMQInterface.connection
        channel = RabbitMQInterface.channel
        
        queues = RabbitMQInterface.queues or []
        consumers = RabbitMQInterface.consumers or []

        for queue in queues:
            channel.queue_declare(queue, durable=True)
        
        for consumer in consumers:
            channel.basic_consume(
                queue = consumer.get('queue'),
                on_message_callback = RabbitMQInterface.onMessageReceived(consumer.get('callback')),
                auto_ack = consumer.get('ack'),
            )
            logging.info(f'[+++] Added consumer "{consumer.get("callback").__name__}" for "{consumer.get("queue")}" queue ')
    
    @staticmethod
    def init():
        if not RabbitMQInterface.channel:
            raise Exception
        RabbitMQInterface.channel.start_consuming()

    @staticmethod
    def createConnection(url):
        logging.info('[---] Connecting to rabbitmq [---]')
        params = URLParameters(url)
        params.socket_timeout = 5
        conn = BlockingConnection(params)
        logging.info('[+++] Connected to rabbitmq  [+++]')
        return conn

    @staticmethod
    def consumer(queue: str, auto_ack = True):
        if not RabbitMQInterface.queues:
            RabbitMQInterface.queues = []
        if not RabbitMQInterface.consumers:
            RabbitMQInterface.consumers = []

        if queue not in RabbitMQInterface.queues:
            RabbitMQInterface.queues.append(queue)

        def wrapper(func):
            RabbitMQInterface.consumers.append({ 'queue': queue, 'callback': func, 'ack': auto_ack })
            return func

        return wrapper

