import json
from io import BytesIO

from libs import images2pdf, mergePDFs
from interface import RabbitMQInterface
from pypdf import PdfReader, PdfWriter
import config
from config import Events


@RabbitMQInterface.consumer(queue = 'images-to-pdf')
def images2pdfConsumer(message):
    urls = message.get('urls')
    pdf = images2pdf(urls)
    
    e = {
        'filename' : message.get('filename'),
        'sessionId': message.get('sessionId'),
        'encoding': 'latin1',
        'pdf_raw_data': pdf.decode('latin1'),
    }
    RabbitMQInterface.publish_event(e, Events.IMAGES_CONVERTED_TO_PDF_EVENT)

@RabbitMQInterface.consumer(queue = 'merge-pdfs')
def mergePDFsConsumer(message):
    urls = message.get('urls')
    pdf = mergePDFs(urls)

    message = {
        'filename': message.get('filename'),
        'sessionId': message.get('sessionId'),
        'pdf_raw_data': pdf.decode('latin1'),
        'encoding': 'latin1',
    }
    RabbitMQInterface.publish_event(e, Events.PDFS_MERGED_EVENT)
