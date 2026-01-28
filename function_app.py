import os
import azure.functions as func
import azure.durable_functions as df
from azure.ai.translation.document import (
    DocumentTranslationClient,
    DocumentTranslationInput,
    TranslationTarget
)
from azure.core.credentials import AzureKeyCredential

myApp = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)


# An HTTP-triggered function with a Durable Functions client binding
@myApp.route(route="orchestrators/{functionName}", methods=["GET", "POST"])
@myApp.durable_client_input(client_name="client")
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
@myApp.orchestration_trigger(context_name="context")
def translate_pdf_orchestrator(context):
    input_data = context.get_input()

    result = yield context.call_activity(
        "translate_pdf_activity",
        input_data
    )

    return result


# Activity
@myApp.activity_trigger(input_name="payload")
def translate_pdf_activity(payload: dict):
    source_blob_url = payload["source_blob_url"]
    source_lang = payload.get("source_lang")
    langs = payload["langs"]

    endpoint = os.environ["DOCUMENT_TRANSLATOR_ENDPOINT"]
    key = os.environ["TRANSLATOR_KEY"]

    target_url = source_blob_url.rsplit("/", 1)[0] + "/translations"
    base_filename = source_blob_url.split("/")[-1]

    client = DocumentTranslationClient(
        endpoint,
        AzureKeyCredential(key)
    )

    targets = [
        TranslationTarget(
            target_url=f"{target_url}/{lang}_{base_filename}",
            language=lang
        )
        for lang in langs
    ]

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
        else:
            documents.append({
                "id": doc.id,
                "status": doc.status,
                "error_code": doc.error.code if doc.error else None,
                "error_message": doc.error.message if doc.error else None,
            })

    return {
        "status": poller.status(),
        "documents": documents
    }