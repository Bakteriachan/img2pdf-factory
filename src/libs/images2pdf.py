from typing import List
from urllib import request
from io import BytesIO
import sys

from fpdf import FPDF
from get_image_size import get_image_size_from_bytesio as ImageSize

def images2pdf(urls: List[str]):
    pdf = FPDF()

    for url in urls:
        pdf.add_page()
        img = request.urlopen(url).read()
        size = len(img)
        b = BytesIO(img)
        w, h = ImageSize(b, size)
        pdf.image(url, w = 0, h = h / 6)
    buffer = pdf.output(dest='S')
    return buffer

