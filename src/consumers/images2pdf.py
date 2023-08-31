import json

from libs import images2pdf
from interface import RabbitMQInterface


@RabbitMQInterface.consumer(queue = 'images-to-pdf')
def images2pdfConsumer(message, channel):
    urls = message.get('urls')
    pdf = images2pdf(urls)
    
    message = json.dumps({
        'requestId': message.get('requestId'),
        'pdf_raw_data': pdf.decode('latin1'),
    })
    channel.queue_declare('images-converted-to-pdf')
    channel.basic_publish(
        exchange = '',
        routing_key = 'images-converted-to-pdf',
        body = message,
    )
