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
    queue_bindings = None

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
    def connect(url, exchange = ''):
        if not RabbitMQInterface.connection:
            RabbitMQInterface.connection = RabbitMQInterface.createConnection(url)
            RabbitMQInterface.channel = RabbitMQInterface.connection.channel()

        connection = RabbitMQInterface.connection
        channel = RabbitMQInterface.channel
        
        queues = RabbitMQInterface.queues or []
        consumers = RabbitMQInterface.consumers or []
        bindings = RabbitMQInterface.queue_bindings or []
        print(queues)
        print(bindings)

        for queue in queues:
            channel.queue_declare(queue, durable=True)
        
        for consumer in consumers:
            channel.basic_consume(
                queue = consumer.get('queue'),
                on_message_callback = RabbitMQInterface.onMessageReceived(consumer.get('callback')),
                auto_ack = consumer.get('ack'),
            )
            logging.info(f'[+++] Added consumer "{consumer.get("callback").__name__}" for "{consumer.get("queue")}" queue ')
        for binding in bindings:
            channel.queue_bind(
                queue = binding.get('queue'),
                exchange = exchange,
                routing_key = binding.get('routing_key'),
            )
            logging.info(f'[+++] added event handler {binding.get("callback").__name__} for event {binding.get("routing_key")} [+++]')

    
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

    @staticmethod
    def onEvent(event: str, auto_ack = True):
        if not RabbitMQInterface.queues:
            RabbitMQInterface.queues = []
        if not RabbitMQInterface.queue_bindings:
            RabbitMQInterface.queue_bindings= []

        def wrapper(func):
            queue_name = f'{func.__name__}-event-consumer'
            RabbitMQInterface.queue_bindings.append(dict(
                queue = queue_name,
                routing_key = event,
                callback = func,
            ))
            RabbitMQInterface.queues.append(queue_name)
            return func

        return wrapper

    @staticmethod
    def publish_event(event: dict, event_name: str):
        body = json.dump(event)
        RabbitMQInterface.channel.basic_publish(
            exchange = config.RABBIT_EXCHANGE,
            body = body,
            routing_key = event_name,
        )

