import azure.functions as func
import azure.durable_functions as df

app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# --- Register functions ---
from functions import translate_pdf
from functions import process_image

translate_pdf.register(app)
process_image.register(app)


