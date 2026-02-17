import os
import azure.functions as func


def validate_bearer_token(req: func.HttpRequest) -> func.HttpResponse | None:
    """Validate Bearer token from the Authorization header.

    Returns an error HttpResponse if invalid, or None if valid.
    """
    auth_header = req.headers.get("Authorization")
    expected_token = os.environ.get("BACKEND_API_KEY")

    if not expected_token:
        return func.HttpResponse(
            '{"error": "Server misconfiguration: missing API key"}',
            status_code=500,
            mimetype="application/json",
        )

    if not auth_header or auth_header != f"Bearer {expected_token}":
        return func.HttpResponse(
            '{"error": "Unauthorized"}',
            status_code=401,
            mimetype="application/json",
        )

    return None
