"""Service Bus triggered push notification sender.

Queue payload shape:
{
    "notification_id": 42
}

Uses Django API endpoints for data access and status updates:
- GET  /api/notifications/{id}/
- PATCH /api/notifications/{id}/
- GET  /api/notifications/{id}/pending-deliveries/
- POST /api/notifications/{id}/delivery-results/

Backend pre-creates MessageDelivery rows (active + inactive devices) before
enqueuing the Service Bus message.  The Function only receives the subset of
deliveries for active devices, sends push attempts, and reports per-delivery
outcomes.  Inactive-device rows remain untouched so users can see missed
messages when they next open the app.
"""

import json
import logging
import os
from typing import Dict, List, Optional, Tuple

import azure.functions as func
TRANSLATIONS_PATH = os.path.join(os.path.dirname(__file__), "..", "translations.json")

def _load_translations() -> Dict:
    with open(TRANSLATIONS_PATH, encoding="utf-8") as f:
        return json.load(f)["notifications"]

def _get_localized_push_fields(notification_type: str, lang: str) -> Tuple[str, str]:
    translations = _load_translations()
    normalized = _normalize_language(lang)
    notif_trans = translations.get(notification_type, {})
    lang_trans = notif_trans.get(normalized) or notif_trans.get("en") or notif_trans.get("fi")
    if lang_trans:
        return lang_trans.get("title", "Uusi ilmoitus"), lang_trans.get("body", "Paina avataksesi")
    return "Uusi ilmoitus", "Paina avataksesi"
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


def _api_get_pending_deliveries(notification_id: int) -> List[Dict]:
    """GET /api/notifications/{id}/pending-deliveries/

    Returns active-device-only deliveries that should receive a push attempt.
    Inactive-device MessageDelivery rows are excluded here but exist in the DB
    for inbox visibility (UserMessagesList).

    Expected item shape: {delivery_id, push_token, lang}
    """
    response = requests.get(
        _backend_url(f"api/notifications/{notification_id}/pending-deliveries/"),
        headers=_backend_headers(),
        timeout=15,
    )
    response.raise_for_status()

    payload = response.json()
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("deliveries", "results"):
            items = payload.get(key)
            if isinstance(items, list):
                return items

    logging.warning(
        "Unexpected pending-deliveries response shape for notification %s",
        notification_id,
    )
    return []


def _api_post_delivery_results(notification_id: int, results: List[Dict]) -> None:
    """POST /api/notifications/{id}/delivery-results/

    Reports per-delivery Expo outcomes back to Django so the backend can:
    - stamp delivered_at on successes
    - record error_code on failures
    - deactivate devices that returned DeviceNotRegistered

    Body: {"results": [{"delivery_id": int, "status": "ok"|"failed",
                        "error_code": str|None}, ...]}
    """
    response = requests.post(
        _backend_url(f"api/notifications/{notification_id}/delivery-results/"),
        json={"results": results},
        headers=_backend_headers(),
        timeout=15,
    )
    response.raise_for_status()


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


def _chunk(items: List, size: int) -> List[List]:
    return [items[i:i + size] for i in range(0, len(items), size)]


def _send_expo_batch(
    deliveries: List[Dict], title: str, body: str
) -> Tuple[List[Dict], List[Dict]]:
    """Send one Expo batch.  Returns (ok_results, failed_results).

    Each result dict: {"delivery_id": int, "status": "ok"|"failed",
                       "error_code": str|None}

    Raises RuntimeError on Expo 5xx so Service Bus can retry the message.
    """
    payload = [
        {"to": d["push_token"], "title": title, "body": body, "sound": "default"}
        for d in deliveries
    ]
    response = requests.post(
        EXPO_PUSH_URL,
        json=payload,
        headers={"Content-Type": "application/json"},
        timeout=20,
    )
    if response.status_code >= 500:
        raise RuntimeError(f"Expo push transient failure: {response.status_code}")

    def _all_failed(error_code: str = None) -> Tuple[List, List]:
        return [], [
            {"delivery_id": d["delivery_id"], "status": "failed", "error_code": error_code}
            for d in deliveries
        ]

    if not response.ok:
        logging.error("Expo push call failed: %s - %s", response.status_code, response.text)
        return _all_failed()

    try:
        tickets = response.json().get("data", [])
        if not isinstance(tickets, list):
            return _all_failed()
    except ValueError:
        logging.error("Expo push response was not valid JSON.")
        return _all_failed()

    ok_results: List[Dict] = []
    failed_results: List[Dict] = []
    for delivery, ticket in zip(deliveries, tickets):
        delivery_id = delivery["delivery_id"]
        if ticket.get("status") == "ok":
            ok_results.append({"delivery_id": delivery_id, "status": "ok", "error_code": None})
        else:
            error_code = None
            if isinstance(ticket.get("details"), dict):
                error_code = ticket["details"].get("error")
            failed_results.append({
                "delivery_id": delivery_id,
                "status": "failed",
                "error_code": error_code,
            })

    return ok_results, failed_results


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
            params = data.get("params") or {}
            notification_type = row.get("notification_type") or params.get("type") or ""

            # Keep statuses aligned with Django model choices.
            _api_patch_notification(notification_id, {"status": "processing"})

            deliveries = _api_get_pending_deliveries(notification_id)

            if not deliveries:
                _api_patch_notification(
                    notification_id,
                    {
                        "status": "completed",
                        "total_devices": 0,
                        "success_count": 0,
                        "failure_count": 0,
                    },
                )
                logging.info("Notification %s: no pending deliveries.", notification_id)
                return

            # Group by language so each batch gets the right localized content.
            by_lang: Dict[str, List[Dict]] = {}
            for delivery in deliveries:
                token = delivery.get("push_token") or delivery.get("token") or ""
                if not token:
                    logging.warning(
                        "Delivery %s has no push token, skipping.",
                        delivery.get("delivery_id"),
                    )
                    continue
                lang = _normalize_language(
                    delivery.get("lang") or delivery.get("language") or "en"
                )
                by_lang.setdefault(lang, []).append(
                    {"delivery_id": delivery["delivery_id"], "push_token": token}
                )

            all_ok: List[Dict] = []
            all_failed: List[Dict] = []

            for lang, lang_deliveries in by_lang.items():
                title, body = _get_localized_push_fields(notification_type, lang)
                for batch in _chunk(lang_deliveries, EXPO_BATCH_SIZE):
                    ok, failed = _send_expo_batch(batch, title, body)
                    all_ok.extend(ok)
                    all_failed.extend(failed)

            # Report per-delivery outcomes so backend can stamp delivered_at,
            # record error codes, and deactivate DeviceNotRegistered devices.
            if all_ok or all_failed:
                _api_post_delivery_results(notification_id, all_ok + all_failed)

            total_sent = len(all_ok)
            total_failed = len(all_failed)
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
