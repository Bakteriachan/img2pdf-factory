from typing import List
from io import BytesIO
import urllib
import fitz

def cbz2pdf(url: str) -> bytes:
    cbz = urllib.request.urlopen(url).read()
    bIO = BytesIO(cbz)
    
    cbzFile = fitz.Document(stream=bIO, filetype='cbz')
    pdf = cbzFile.convert_to_pdf()
    
    return pdf

def mergeCbz2Pdf(urls: List[str]) -> bytes:
    pdf = None
    for url in urls:
        converted_cbz = cbz2pdf(url)
        if not pdf:
            pdf = fitz.Document(stream = BytesIO(converted_cbz), filetype = 'pdf')
        else:
            pdf.insert_pdf(fitz.Document(stream = BytesIO(converted_cbz), filetype = 'pdf'))

    return pdf.tobytes()

