from interface import RabbitMQInterface
import consumers

def main():
    try:
        RabbitMQInterface.connect('amqp://149.100.154.239:5672')
        RabbitMQInterface.init()
    except Exception as e:
        raise

if __name__ == '__main__':
    main()
