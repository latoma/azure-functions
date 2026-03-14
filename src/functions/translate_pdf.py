"""PDF Translation Workflow

Durable Functions workflow: HTTP trigger -> orchestrator -> translate + notify activities.
"""

import os
import logging
import requests
import azure.functions as func
from shared.auth import validate_bearer_token
from shared.translation import translate_document


def register(app):
    """Register all PDF translation functions."""

    # --- HTTP Trigger ---

    @app.route(route="translate-pdf", methods=["POST"])
    @app.durable_client_input(client_name="client")
    async def translate_pdf_http(req: func.HttpRequest, client):

        error_response = validate_bearer_token(req)
        if error_response:
            return error_response

        try:
            body = req.get_json()
        except ValueError:
            return func.HttpResponse("Invalid JSON body", status_code=400)

        required_fields = ["source_blob_url", "langs", "pdf_attachment_id"]
        missing = [f for f in required_fields if f not in body]

        if missing:
            return func.HttpResponse(
                f"Missing required fields: {missing}",
                status_code=400,
            )

        payload = {
            "source_blob_url": body["source_blob_url"],
            "source_lang": body.get("source_lang", "fi"),
            "langs": body["langs"],
            "pdf_attachment_id": body["pdf_attachment_id"],
            "blob_name": body.get(
                "blob_name",
                body["source_blob_url"].split("/")[-1],
            ),
        }

        instance_id = await client.start_new(
            "translate_pdf_orchestrator",
            None,
            payload,
        )

        logging.info(f"Started translate_pdf orchestration: {instance_id}")
        return client.create_check_status_response(req, instance_id)

    # --- Orchestrator ---

    @app.orchestration_trigger(context_name="context")
    def translate_pdf_orchestrator(context):
        input_data = context.get_input()

        if not context.is_replaying:
            logging.info(
                f"Starting PDF translation orchestration for: {input_data.get('blob_name')}"
            )

        langs = input_data.get("langs", [])

        if not langs:
            return {
                "documents": [],
                "blob_names": {},
                "pdf_attachment_id": input_data["pdf_attachment_id"],
            }

        chunk_size = 10

        lang_chunks = [
            langs[i:i + chunk_size]
            for i in range(0, len(langs), chunk_size)
        ]

        all_documents = []
        all_blob_names = {}

        for chunk in lang_chunks:
            chunk_payload = dict(input_data)
            chunk_payload["langs"] = chunk

            result = yield context.call_activity(
                "translate_pdf_activity",
                chunk_payload,
            )

            all_documents.extend(result.get("documents", []))
            all_blob_names.update(result.get("blob_names", {}))

        final_result = {
            "documents": all_documents,
            "blob_names": all_blob_names,
            "pdf_attachment_id": input_data["pdf_attachment_id"],
        }

        yield context.call_activity(
            "notify_backend_activity",
            final_result,
        )

        return final_result

    # --- Activities ---

    @app.activity_trigger(input_name="payload")
    def translate_pdf_activity(payload: dict):
        """Translate a PDF using Azure Document Translation."""

        source_blob_url = payload["source_blob_url"]
        source_lang = payload.get("source_lang", "fi")
        langs = payload["langs"]
        blob_name = payload.get("blob_name", source_blob_url.split("/")[-1])

        documents, blob_names = translate_document(
            source_blob_url=source_blob_url,
            source_lang=source_lang,
            langs=langs,
            blob_name=blob_name,
        )

        return {
            "documents": documents,
            "blob_names": blob_names,
            "pdf_attachment_id": payload["pdf_attachment_id"],
        }

    @app.activity_trigger(input_name="payload")
    def notify_backend_activity(payload: dict):
        """Notify the Django backend that translation is complete."""

        pdf_attachment_id = payload.get("pdf_attachment_id")

        try:
            response = requests.patch(
                f"{os.environ['BACKEND_URL']}api/functions/patch-pdf-translations/{pdf_attachment_id}/",
                json={"blob_names": payload["blob_names"]},
                headers={"Authorization": f"Bearer {os.environ['BACKEND_API_KEY']}"},
                timeout=10,
            )
            response.raise_for_status()
            logging.info(f"Backend notified for attachment {pdf_attachment_id}")
        except requests.RequestException as e:
            logging.error(
                f"Failed to notify backend for attachment {pdf_attachment_id}: {e}"
            )
            raise
