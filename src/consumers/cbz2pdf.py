import json

from interface import RabbitMQInterface
from libs import cbz2pdf, mergeCbz2Pdf
import config
from config import Events

@RabbitMQInterface.consumer(queue = 'convert-cbz-to-pdf')
def cbz2pdfConsumer(message: dict, channel):
    url = message.get('url')
    pdf = cbz2pdf(url).decode('latin1')
    e = dict(
        filename = message.get('filename'),
        sessionId = message.get('sessionId'),
        raw_pdf_data = pdf,
        encoding = 'latin1',
    )
    RabbitMQInterface.publish_event(e, Events.CBZ_CONVERTED_TO_PDF_EVENT)    

@RabbitMQInterface.consumer(queue = 'merge-cbz-to-pdf')
def mergeCbz2PdfConsumer(message: dict):
    urls = message.get('urls')
    pdf = mergeCbz2Pdf(urls)
    r = dict(
        filename = message.get('filename'),
        sessionId = message.get('sessionId'),
        raw_pdf_data = pdf.decode('latin1'),
        encoding = 'latin1'          
    )
    RabbitMQInterface.publish_event(r, Events.MERGED_CBZ_TO_PDF_EVENT)
