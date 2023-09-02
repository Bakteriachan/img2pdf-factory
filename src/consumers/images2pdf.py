import json
from io import BytesIO

from libs import images2pdf, mergePDFs
from interface import RabbitMQInterface
from pypdf import PdfReader, PdfWriter
import config


@RabbitMQInterface.consumer(queue = 'images-to-pdf')
def images2pdfConsumer(message, channel):
    urls = message.get('urls')
    pdf = images2pdf(urls)
    
    message = json.dumps({
        'requestId': message.get('requestId'),
        'encoding': 'latin1',
        'pdf_raw_data': pdf.decode('latin1'),
    })
    channel.basic_publish(
        exchange = config.RABBIT_EXCHANGE,
        routing_key = 'images-converted-to-pdf',
        body = message,
    )

@RabbitMQInterface.consumer(queue = 'merge-pdfs')
def mergePDFsConsumer(message, channel):
    urls = message.get('urls')
    pdf = mergePDFs(urls)

    message = json.dumps({
        'requestId': message.get('requestId'),
        'pdf_raw_data': pdf.decode('latin1'),
        'encoding': 'latin1',
    })
    channel.basic_publish(
        exchange = config.RABBIT_EXCHANGE,
        routing_key = 'pdfs-mergeds',
        body = message,
    )

