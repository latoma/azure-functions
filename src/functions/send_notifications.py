"""Service Bus triggered push notification sender.

Queue payload shape:
{
    "notification_id": 42
}

Uses Django API endpoints for data access and status updates:
- GET /api/notifications/{id}/
- PATCH /api/notifications/{id}/
- GET /api/notifications/{id}/target-devices/
"""

import json
import logging
import os
from typing import Dict, List, Optional, Tuple

import azure.functions as func
import requests


EXPO_PUSH_URL = "https://exp.host/--/api/v2/push/send"
EXPO_BATCH_SIZE = 100


def _backend_url(path: str) -> str:
    base_url = os.environ["BACKEND_URL"].rstrip("/")
    return f"{base_url}/{path.lstrip('/')}"


def _backend_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {os.environ['BACKEND_API_KEY']}",
        "Content-Type": "application/json",
    }


def _api_get_notification(notification_id: int) -> Optional[Dict]:
    response = requests.get(
        _backend_url(f"api/notifications/{notification_id}/"),
        headers=_backend_headers(),
        timeout=10,
    )
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return response.json()


def _api_patch_notification(notification_id: int, payload: Dict) -> None:
    response = requests.patch(
        _backend_url(f"api/notifications/{notification_id}/"),
        json=payload,
        headers=_backend_headers(),
        timeout=10,
    )
    response.raise_for_status()


def _api_get_target_devices(notification_id: int) -> List[Dict]:
    response = requests.get(
        _backend_url(f"api/notifications/{notification_id}/target-devices/"),
        headers=_backend_headers(),
        timeout=15,
    )
    response.raise_for_status()

    payload = response.json()
    if isinstance(payload, list):
        return payload

    if isinstance(payload, dict):
        results = payload.get("results")
        if isinstance(results, list):
            return results

    logging.warning(
        "Unexpected target-devices response shape for notification %s",
        notification_id,
    )
    return []



def _parse_queue_message(msg: func.ServiceBusMessage) -> Optional[int]:
    try:
        payload = json.loads(msg.get_body().decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        logging.error("Invalid queue message body. Expected JSON with notification_id.")
        return None

    notification_id = payload.get("notification_id")
    if not isinstance(notification_id, int) or notification_id <= 0:
        logging.error("Invalid notification_id in queue payload: %s", notification_id)
        return None

    return notification_id


def _normalize_language(lang: str) -> str:
    if not lang:
        return "en"
    return lang.split("-", 1)[0].lower()


def _pick_localized_text(
    lang: str,
    title_translations: Dict[str, str],
    body_translations: Dict[str, str],
    fallback_title: str,
    fallback_body: str,
) -> Tuple[str, str]:
    normalized = _normalize_language(lang)
    title = (
        title_translations.get(normalized)
        or title_translations.get("en")
        or title_translations.get("fi")
        or fallback_title
    )
    body = (
        body_translations.get(normalized)
        or body_translations.get("en")
        or body_translations.get("fi")
        or fallback_body
    )
    return title, body


def _chunk(items: List[str], size: int) -> List[List[str]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


def _send_expo_batch(tokens: List[str], title: str, body: str) -> Tuple[int, int]:
    payload = [
        {"to": token, "title": title, "body": body, "sound": "default"}
        for token in tokens
    ]
    response = requests.post(
        EXPO_PUSH_URL,
        json=payload,
        headers={"Content-Type": "application/json"},
        timeout=20,
    )
    if response.status_code >= 500:
        raise RuntimeError(f"Expo push transient failure: {response.status_code}")

    if not response.ok:
        logging.error("Expo push call failed: %s - %s", response.status_code, response.text)
        return 0, len(tokens)

    try:
        tickets = response.json().get("data", [])
        if not isinstance(tickets, list):
            return 0, len(tokens)
    except ValueError:
        logging.error("Expo push response was not valid JSON.")
        return 0, len(tokens)

    sent = sum(1 for t in tickets if t.get("status") == "ok")
    failed = len(tokens) - sent
    return sent, max(failed, 0)


def register(app):
    @app.service_bus_queue_trigger(
        arg_name="msg",
        queue_name="send-notification",
        connection="SERVICE_BUS_CONNECTION",
    )
    def send_notification_from_queue(msg: func.ServiceBusMessage):
        notification_id = _parse_queue_message(msg)
        if notification_id is None:
            return

        try:
            row = _api_get_notification(notification_id)

            if not row:
                logging.warning("Notification %s not found.", notification_id)
                return

            if row.get("status") not in ("pending", "failed"):
                logging.info(
                    "Notification %s already '%s', skipping.",
                    notification_id,
                    row.get("status"),
                )
                return

            data = row.get("data") or {}
            title_translations = data.get("title_translations", {})
            body_translations = data.get("body_translations", {})

            # Keep statuses aligned with Django model choices.
            _api_patch_notification(notification_id, {"status": "processing"})

            devices = _api_get_target_devices(notification_id)

            if not devices:
                _api_patch_notification(
                    notification_id,
                    {
                        "status": "completed",
                        "total_devices": 0,
                        "success_count": 0,
                        "failure_count": 0,
                    },
                )
                logging.info("Notification %s: no target devices.", notification_id)
                return

            by_lang: Dict[str, List[str]] = {}
            for device in devices:
                token = device.get("push_token") or device.get("token") or ""
                if not token:
                    continue
                lang = _normalize_language(device.get("lang") or device.get("language") or "en")
                by_lang.setdefault(lang, []).append(token)

            total_sent = 0
            total_failed = 0
            for lang, tokens in by_lang.items():
                title, body = _pick_localized_text(
                    lang,
                    title_translations,
                    body_translations,
                    fallback_title=row.get("title") or "Notification",
                    fallback_body=row.get("body") or "",
                )
                for batch in _chunk(tokens, EXPO_BATCH_SIZE):
                    sent, failed = _send_expo_batch(batch, title, body)
                    total_sent += sent
                    total_failed += failed

            final_status = "completed" if total_sent > 0 or total_failed == 0 else "failed"
            _api_patch_notification(
                notification_id,
                {
                    "status": final_status,
                    "total_devices": total_sent + total_failed,
                    "success_count": total_sent,
                    "failure_count": total_failed,
                },
            )
            logging.info(
                "Notification %s done: status=%s sent=%s failed=%s",
                notification_id, final_status, total_sent, total_failed,
            )

        except Exception:
            logging.exception("Error processing notification %s", notification_id)
            try:
                _api_patch_notification(notification_id, {"status": "failed"})
            except Exception:
                logging.exception("Failed to mark notification %s as failed", notification_id)
            raise
