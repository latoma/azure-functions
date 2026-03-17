import os
import logging
from azure.ai.translation.document import (
    DocumentTranslationClient,
    DocumentTranslationInput,
    TranslationTarget,
)
from azure.core.credentials import AzureKeyCredential


def translate_document(
    source_blob_url: str,
    source_lang: str,
    langs: list[str],
    blob_name: str,
) -> tuple[list[dict], dict]:
    """Translate a document using Azure Document Translation.

    Args:
        source_blob_url: Full URL to the source blob.
        source_lang: Source language code (e.g. "fi").
        langs: List of target language codes (e.g. ["en", "sv"]).
        blob_name: Full blob path (e.g. "discussion/uuid/file.pdf").

    Returns:
        Tuple of (documents list, blob_names dict keyed by language code).
    """
    endpoint = os.environ["DOCUMENT_TRANSLATOR_ENDPOINT"]
    key = os.environ["TRANSLATOR_KEY"]

    # Parse storage account URL and extract container
    # Example: https://hazardhuntdevstorage.blob.core.windows.net/attachments/discussion/<uuid>/file.pdf
    url_parts = source_blob_url.split("/")
    storage_base_url = f"{url_parts[0]}//{url_parts[2]}"
    container = url_parts[3]

    blob_parts = blob_name.split("/")
    filename = blob_parts[-1]
    directory_path = "/".join(blob_parts[:-1])

    client = DocumentTranslationClient(
        endpoint,
        AzureKeyCredential(key),
    )

    # Build translation targets - store in translations subfolder
    targets = [
        TranslationTarget(
            target_url=f"{storage_base_url}/{container}/{directory_path}/translations/{lang}_{filename}",
            language=lang,
        )
        for lang in langs
    ]

    logging.info(f"Starting translation for {blob_name} from {source_lang} to {langs}")

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
    blob_names = {}

    for doc in result:
        if doc.status == "Succeeded":
            language = doc.translated_to[:2]
            translated_blob_name = f"{directory_path}/translations/{language}_{filename}"
            blob_names[language] = translated_blob_name

            documents.append({
                "id": doc.id,
                "language": language,
                "status": doc.status,
                "source_url": doc.source_document_url,
                "translated_url": doc.translated_document_url,
            })
            logging.info(f"Translation succeeded: {doc.translated_document_url}")
        else:
            documents.append({
                "id": doc.id,
                "status": doc.status,
                "error_code": doc.error.code if doc.error else None,
                "error_message": doc.error.message if doc.error else None,
            })
            logging.error(
                f"Translation failed for {doc.id}: "
                f"{doc.error.message if doc.error else 'Unknown error'}"
            )

    return documents, blob_names
