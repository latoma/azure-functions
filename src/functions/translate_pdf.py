"""PDF Translation Workflow

Durable Functions workflow: Service Bus trigger -> orchestrator -> translate + notify activities.

Queue message shape (translate-pdf queue):
{
    "source_blob_url": "https://...",
    "source_lang": "fi",
    "langs": ["en", "sv"],
    "pdf_attachment_id": 42,
    "blob_name": "discussion/20260317_120000/form.pdf"  # optional
}
"""

import json
import os
import logging
import requests
import azure.functions as func
from shared.translation import translate_document


STATUS_COMPLETED = "completed"
STATUS_FAILED = "failed"
STATUS_TRANSLATING = "translating"


def register(app):
    """Register all PDF translation functions."""

    # --- Service Bus Queue Trigger ---
    @app.service_bus_queue_trigger(
        arg_name="msg",
        queue_name="translate-pdf",
        connection="SERVICE_BUS_CONNECTION",
    )
    @app.durable_client_input(client_name="client")
    async def translate_pdf_queue(msg: func.ServiceBusMessage, client):
        try:
            body = json.loads(msg.get_body().decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            logging.error("Invalid queue message body for PDF translation — message will be dead-lettered")
            return

        required_fields = ["source_blob_url", "langs", "pdf_attachment_id"]
        missing = [f for f in required_fields if f not in body]
        if missing:
            logging.error("PDF translation message missing required fields: %s — dead-lettering", missing)
            return

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

        # Mark as actively translating only after orchestration starts successfully.
        try:
            response = requests.patch(
                f"{os.environ['BACKEND_URL']}api/functions/patch-pdf-translations/{payload['pdf_attachment_id']}/",
                json={"status": STATUS_TRANSLATING},
                headers={"Authorization": f"Bearer {os.environ['BACKEND_API_KEY']}"},
                timeout=10,
            )
            response.raise_for_status()
        except requests.RequestException as e:
            logging.error(
                "Failed to set translating status for attachment %s: %s",
                payload["pdf_attachment_id"],
                e,
            )

        logging.info("Started translate_pdf orchestration %s for attachment %s", instance_id, payload["pdf_attachment_id"])

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
            final_result = {
                "documents": [],
                "blob_names": {},
                "pdf_attachment_id": input_data["pdf_attachment_id"],
                "status": STATUS_COMPLETED,
            }
            yield context.call_activity("notify_backend_activity", final_result)
            return final_result

        chunk_size = 10

        lang_chunks = [
            langs[i:i + chunk_size]
            for i in range(0, len(langs), chunk_size)
        ]

        all_documents = []
        all_blob_names = {}

        try:
            chunk_payloads = [dict(input_data, langs=chunk) for chunk in lang_chunks]
            tasks = [context.call_activity("translate_pdf_activity", p) for p in chunk_payloads]
            results = yield context.task_all(tasks)

            for result in results:
                all_documents.extend(result.get("documents", []))
                all_blob_names.update(result.get("blob_names", {}))
        except Exception as e:
            yield context.call_activity(
                "notify_backend_activity",
                {
                    "pdf_attachment_id": input_data["pdf_attachment_id"],
                    "status": STATUS_FAILED,
                    "error": str(e),
                    "blob_names": all_blob_names,
                },
            )
            raise

        final_result = {
            "documents": all_documents,
            "blob_names": all_blob_names,
            "pdf_attachment_id": input_data["pdf_attachment_id"],
            "status": STATUS_COMPLETED,
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
        """Notify the Django backend about PDF translation state changes."""

        pdf_attachment_id = payload.get("pdf_attachment_id")
        body = {
            "status": payload.get("status", STATUS_COMPLETED),
            "blob_names": payload.get("blob_names", {}),
        }
        if "error" in payload:
            body["error"] = payload.get("error")

        try:
            response = requests.patch(
                f"{os.environ['BACKEND_URL']}api/functions/patch-pdf-translations/{pdf_attachment_id}/",
                json=body,
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
