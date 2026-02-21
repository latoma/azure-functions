"""Event Grid triggered image compressor.

Listens for BlobCreated events, compresses the image in-place
(resize + JPEG optimize) so it's ready for React Native consumption.
"""

import os
import logging
import azure.functions as func
from azure.storage.blob import ContentSettings
from shared.storage import get_blob_service_client
from shared.image_processing import compress_image

SUPPORTED_EXTENSIONS = {".jpg", ".jpeg", ".png", ".bmp", ".tiff", ".tif", ".gif", ".webp"}


def register(app):

    @app.event_grid_trigger(arg_name="event")
    def process_image_on_upload(event: func.EventGridEvent):
        """Compress an uploaded image and overwrite the original blob."""

        if event.event_type != "Microsoft.Storage.BlobCreated":
            return

        # Subject: /blobServices/default/containers/{container}/blobs/{name}
        try:
            after = event.subject.split("/containers/", 1)[1]
            container, blob_name = after.split("/blobs/", 1)
        except (IndexError, ValueError):
            logging.warning(f"Unparseable subject: {event.subject}")
            return

        # Only process supported image formats
        ext = os.path.splitext(blob_name)[1].lower()
        if ext not in SUPPORTED_EXTENSIONS:
            return

        blob_service = get_blob_service_client()
        blob_client = blob_service.get_blob_client(container, blob_name)

        # Get blob properties to check content type
        blob_properties = blob_client.get_blob_properties()

        # Skip if already processed (WebP) to prevent infinite loop
        if blob_properties.content_settings.content_type == "image/webp":
            logging.info(f"Skipping {container}/{blob_name} - already compressed to WebP")
            return

        # Download original
        original = blob_client.download_blob().readall()

        # Compress
        compressed = compress_image(original)

        # Overwrite with compressed WebP version (keep original name for URL consistency)
        blob_client.upload_blob(
            compressed,
            overwrite=True,
            content_settings=ContentSettings(content_type="image/webp"),
        )

        reduction = round((1 - len(compressed) / len(original)) * 100, 1) if original else 0
        logging.info(
            f"Compressed {container}/{blob_name}: "
            f"{len(original) / 1024:.0f}KB -> {len(compressed) / 1024:.0f}KB "
            f"({reduction}% smaller) [WebP format]"
        )
