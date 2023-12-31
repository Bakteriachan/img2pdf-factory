import os

from interface import RabbitMQInterface
import consumers
import telegram
import config

RABBITMQ_URL=os.getenv('RABBITMQ_URL')

def main():
    try:
        RabbitMQInterface.connect(RABBITMQ_URL, exchange = config.RABBIT_EXCHANGE)
        RabbitMQInterface.init()
    except Exception as e:
        raise

if __name__ == '__main__':
    main()
