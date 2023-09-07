from io import BytesIO

from pyrogram import Client

from interface import RabbitMQInterface
from libs import SessionService 
from config import Events
import config

@RabbitMQInterface.onEvent(event = Events.CBZ_CONVERTED_TO_PDF_EVENT)
def sendPdfFileWhenCbzConvertedToPdf(message: dict):
    bot = Client(
        api_id = config.TELEGRAM_API_ID,
        api_hash = config.TELEGRAM_API_HASH,
        bot_token = config.TELEGRAM_BOT_TOKEN,
    )
    
    pdf_document = BytesIO(
        bytes(message.get('raw_pdf_data'), encoding = message.get('encoding'))
    )
    pdf_document.name = message.get('filename')

    session_service = SessionService(config.SESSIONS_SERVICE_URL)
    session = session_service.get_session_by_id(message.get('sessionId'))

    with bot:
        bot.send_document(
            chat_id = session.get('sessionPayload').get('telegramUserId'),
            document = pdf_document,
            file_name = pdf_document.name,
        )
    
