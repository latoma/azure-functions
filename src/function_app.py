import os
import logging
import azure.functions as func
import azure.durable_functions as df
from azure.ai.translation.document import (
    DocumentTranslationClient,
    DocumentTranslationInput,
    TranslationTarget
)
from azure.core.credentials import AzureKeyCredential

app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)


# Event Grid trigger that processes blob upload events
@app.event_grid_trigger(arg_name="azdEvent")
@app.durable_client_input(client_name="client")
async def process_blob_upload(azdEvent: func.EventGridEvent, client):
    """
    Process blob upload event from Event Grid.

    This function triggers when a new blob is created in the unprocessed-pdf container.
    It starts the translation orchestrator to translate the PDF into target languages.
    """

    # Extract event data
    event_data = azdEvent.get_json()
    subject = azdEvent.subject

    # Extract blob name from subject: /blobServices/default/containers/unprocessed-pdf/blobs/{name}
    blob_name = subject.split('/')[-1]

    # Extract blob URL from event data
    source_blob_url = event_data.get('url', '')

    logging.info(f'Python Event Grid Trigger processed blob\n Name: {blob_name}')

    # Get translation configuration from environment variables
    source_lang = os.environ.get("SOURCE_LANG", "fi")
    target_langs = os.environ.get("TARGET_LANGS", "en,sv").split(",")

    # Prepare payload for orchestrator
    payload = {
        "source_blob_url": source_blob_url,
        "source_lang": source_lang,
        "langs": target_langs,
        "blob_name": blob_name
    }

    # Start the translation orchestrator
    instance_id = await client.start_new("translate_pdf_orchestrator", None, payload)
    logging.info(f'Started translation orchestration with instance ID: {instance_id}')


# HTTP endpoint to manually start translation or check status
@app.route(route="orchestrators/{functionName}", methods=["GET", "POST"])
@app.durable_client_input(client_name="client")
async def http_start(req: func.HttpRequest, client):
    function_name = req.route_params["functionName"]

    if req.method == "GET":
        return func.HttpResponse(
            "This endpoint starts a Durable orchestration.\n\n"
            "Use POST with JSON body:\n"
            "{\n"
            '  "source_blob_url": "...",\n'
            '  "source_lang": "fi",\n'
            '  "langs": ["en", "sv"]\n'
            "}",
            status_code=400
        )

    try:
        body = req.get_json()
    except ValueError:
        return func.HttpResponse(
            "Invalid or missing JSON body",
            status_code=400
        )

    instance_id = await client.start_new(function_name, None, body)
    return client.create_check_status_response(req, instance_id)


# Orchestrator
@app.orchestration_trigger(context_name="context")
def translate_pdf_orchestrator(context):
    input_data = context.get_input()

    result = yield context.call_activity(
        "translate_pdf_activity",
        input_data
    )

    return result


# Activity
@app.activity_trigger(input_name="payload")
def translate_pdf_activity(payload: dict):
    source_blob_url = payload["source_blob_url"]
    source_lang = payload.get("source_lang")
    langs = payload["langs"]
    blob_name = payload.get("blob_name", source_blob_url.split("/")[-1])

    endpoint = os.environ["DOCUMENT_TRANSLATOR_ENDPOINT"]
    key = os.environ["TRANSLATOR_KEY"]

    # Parse storage account URL to extract container and directory structure
    # Example: https://hazardhuntdevstorage.blob.core.windows.net/pdfs/discussion/20260130_185154/Telinekataja-safety-observation-2026-01-30.pdf
    url_parts = source_blob_url.split("/")
    storage_base_url = f"{url_parts[0]}//{url_parts[2]}"  # https://accountname.blob.core.windows.net

    # Extract container and path (everything after storage account)
    # url_parts[3:] = ['pdfs', 'discussion', '20260130_185154', 'Telinekataja-safety-observation-2026-01-30.pdf']
    blob_path_parts = url_parts[3:]

    # Get directory path (all but the filename) and filename
    directory_path = "/".join(blob_path_parts[:-1])  # pdfs/discussion/20260130_185154

    client = DocumentTranslationClient(
        endpoint,
        AzureKeyCredential(key)
    )

    # Store translated PDFs in translations subfolder: pdfs/discussion/{timestamp}/translations/{lang}_{filename}.pdf
    targets = [
        TranslationTarget(
            target_url=f"{storage_base_url}/{directory_path}/translations/{lang}_{blob_name}",
            language=lang
        )
        for lang in langs
    ]

    logging.info(f'Starting translation for {blob_name} from {source_lang} to {langs}')

    poller = client.begin_translation(
        inputs=[
            DocumentTranslationInput(
                source_url=source_blob_url,
                source_language=source_lang,
                targets=targets,
                storage_type="file",
            )
        ]
    )

    result = poller.result()

    documents = []

    for doc in result:
        if doc.status == "Succeeded":
            documents.append({
                "id": doc.id,
                "language": doc.translated_to[:2],
                "status": doc.status,
                "source_url": doc.source_document_url,
                "translated_url": doc.translated_document_url,
            })
            logging.info(f'Translation succeeded: {doc.translated_document_url}')
        else:
            documents.append({
                "id": doc.id,
                "status": doc.status,
                "error_code": doc.error.code if doc.error else None,
                "error_message": doc.error.message if doc.error else None,
            })
            logging.error(f'Translation failed for {doc.id}: {doc.error.message if doc.error else "Unknown error"}')

    return {
        "status": poller.status(),
        "documents": documents
    }
