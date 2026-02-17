"""Image Processing Function

Simple HTTP-triggered function for processing uploaded images:
resize, compress, convert to WebP, and strip EXIF metadata.
"""

import os
import json
import logging
import azure.functions as func
from azure.storage.blob import ContentSettings
from shared.auth import validate_bearer_token
from shared.storage import get_blob_service_client
from shared.image_processing import process_image_bytes


SUPPORTED_EXTENSIONS = {".jpg", ".jpeg", ".png", ".bmp", ".tiff", ".tif", ".gif", ".webp"}


def register(app):
    """Register image processing HTTP triggers."""

    # --- Blob Trigger (auto-process on upload) ---

    @app.blob_trigger(
        arg_name="blob",
        path="pictures/images/{name}",
        connection="AzureWebJobsStorage"
    )
    def process_image_on_upload(blob: func.InputStream):
        """Auto-process images when uploaded to 'pictures/images/' path.

        Triggers on any file upload. Processes supported image formats,
        ignores others and already-processed .webp files.
        """
        blob_name = blob.name.split("/", 1)[1]  # Remove container prefix: "pictures/images/file.jpg" -> "images/file.jpg"
        container = "pictures"

        # Skip if already WebP
        if blob_name.endswith(".webp"):
            logging.info(f"Skipping already-processed WebP: {blob_name}")
            return

        # Check if supported format
        ext = os.path.splitext(blob_name)[1].lower()
        if ext not in SUPPORTED_EXTENSIONS:
            logging.info(f"Skipping unsupported format: {blob_name}")
            return

        try:
            logging.info(f"Auto-processing uploaded image: {blob_name}")
            result = _process_blob_image(container, blob_name)
            logging.info(f"Auto-processed: {result['processed_blob']} ({result['reduction_percent']}% reduction)")
        except Exception as e:
            logging.error(f"Auto-processing failed for {blob_name}: {e}")

    @app.route(route="process-image-batch", methods=["POST"])
    def process_image_batch_http(req: func.HttpRequest) -> func.HttpResponse:
        """Process multiple images in a single request.

        Expects JSON body:
        {
            "container": "images",
            "blob_names": ["path/to/photo1.jpg", "path/to/photo2.png"]
        }
        """
        error_response = validate_bearer_token(req)
        if error_response:
            return error_response

        try:
            body = req.get_json()
        except ValueError:
            return func.HttpResponse("Invalid JSON body", status_code=400)

        container = body.get("container")
        blob_names = body.get("blob_names", [])

        if not container or not blob_names:
            return func.HttpResponse(
                '{"error": "Missing required fields: container, blob_names"}',
                status_code=400,
                mimetype="application/json",
            )

        results = []
        for blob_name in blob_names:
            ext = os.path.splitext(blob_name)[1].lower()
            if ext not in SUPPORTED_EXTENSIONS:
                results.append({
                    "blob_name": blob_name,
                    "status": "skipped",
                    "reason": f"Unsupported format: {ext}",
                })
                continue

            try:
                result = _process_blob_image(container, blob_name)
                results.append(result)
            except Exception as e:
                logging.error(f"Failed to process {blob_name}: {e}")
                results.append({
                    "blob_name": blob_name,
                    "status": "error",
                    "error": str(e),
                })

        return func.HttpResponse(
            json.dumps({"container": container, "results": results}),
            status_code=200,
            mimetype="application/json",
        )


def _process_blob_image(container: str, blob_name: str) -> dict:
    """Download, process, and re-upload an image as WebP."""

    blob_service = get_blob_service_client()
    container_client = blob_service.get_container_client(container)

    # Download original
    blob_data = container_client.download_blob(blob_name).readall()
    original_size = len(blob_data)

    logging.info(
        f"Processing image: {container}/{blob_name} "
        f"({original_size / 1024:.1f} KB)"
    )

    # Process
    processed_data = process_image_bytes(blob_data)
    processed_size = len(processed_data)

    # Upload as .webp
    base_name = os.path.splitext(blob_name)[0]
    new_blob_name = f"{base_name}.webp"

    container_client.upload_blob(
        name=new_blob_name,
        data=processed_data,
        overwrite=True,
        content_settings=ContentSettings(content_type="image/webp"),
    )

    # Delete original if extension changed
    if new_blob_name != blob_name:
        try:
            container_client.delete_blob(blob_name)
            logging.info(f"Deleted original blob: {blob_name}")
        except Exception as del_err:
            logging.warning(f"Could not delete original blob {blob_name}: {del_err}")

    reduction = round((1 - processed_size / original_size) * 100, 1) if original_size > 0 else 0

    logging.info(
        f"Processed {blob_name} -> {new_blob_name}: "
        f"{original_size / 1024:.1f} KB -> {processed_size / 1024:.1f} KB "
        f"({reduction}% reduction)"
    )

    return {
        "status": "success",
        "original_blob": blob_name,
        "processed_blob": new_blob_name,
        "container": container,
        "original_size_kb": round(original_size / 1024, 1),
        "processed_size_kb": round(processed_size / 1024, 1),
        "reduction_percent": reduction,
    }
