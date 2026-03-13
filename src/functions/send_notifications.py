"""Service Bus triggered push notification sender.

Queue payload shape:
{
  "notification_id": 42
}

The notification row's `data` JSON field drives targeting:
  target_type        ALL | ACCOUNT | FILTER
  target_filters     {"account_id": 1}
  title_translations {"fi": "...", "en": "..."}
  body_translations  {"fi": "...", "en": "..."}

Falls back to the notification's `title`/`body` columns when translations
are absent.
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import azure.functions as func
import psycopg2
import psycopg2.extras
import requests


EXPO_PUSH_URL = "https://exp.host/--/api/v2/push/send"
EXPO_BATCH_SIZE = 100


def _db_conn():
    return psycopg2.connect(os.environ["AZURE_POSTGRESQL_CONNECTIONSTRING"])



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
        queue_name="%NOTIFICATION_QUEUE_NAME%",
        connection="SERVICE_BUS_CONNECTION",
    )
    def send_notification_from_queue(msg: func.ServiceBusMessage):
        notification_id = _parse_queue_message(msg)
        if notification_id is None:
            return

        conn = _db_conn()
        try:
            # ── Load notification ────────────────────────────────────────────
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT id, title, body, data, notification_type, status
                    FROM api_notification WHERE id = %s
                    """,
                    (notification_id,),
                )
                row = cur.fetchone()

            if not row:
                logging.warning("Notification %s not found.", notification_id)
                return

            if row["status"] not in ("pending", "failed"):
                logging.info(
                    "Notification %s already '%s', skipping.",
                    notification_id, row["status"],
                )
                return

            data = row["data"] or {}
            target_type = data.get("target_type", "ALL").upper()
            target_filters = data.get("target_filters", {})
            title_translations = data.get("title_translations", {})
            body_translations = data.get("body_translations", {})

            # ── Mark as sending ──────────────────────────────────────────────
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE api_notification SET status='sending' WHERE id = %s",
                    (notification_id,),
                )
            conn.commit()

            # ── Resolve target devices ───────────────────────────────────────
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                if target_type == "ALL":
                    cur.execute(
                        """
                        SELECT push_token, lang FROM api_device
                        WHERE is_active = TRUE AND push_token <> ''
                        """
                    )
                elif target_type == "ACCOUNT":
                    cur.execute(
                        """
                        SELECT push_token, lang FROM api_device
                        WHERE is_active = TRUE AND push_token <> ''
                          AND account_id = %s
                        """,
                        (target_filters.get("account_id"),),
                    )
                # elif target_type == "FILTER":
                    # not implemented. idea is to target user groups, like by company_id from account's person relation
                else:
                    logging.error(
                        "Unknown target_type '%s' for notification %s",
                        target_type, notification_id,
                    )
                    return

                devices = cur.fetchall()

            if not devices:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE api_notification
                        SET status='sent', total_devices=0, completed_at=%s
                        WHERE id = %s
                        """,
                        (datetime.now(timezone.utc), notification_id),
                    )
                conn.commit()
                logging.info("Notification %s: no target devices.", notification_id)
                return

            # ── Group by language and send ───────────────────────────────────
            by_lang: Dict[str, List[str]] = {}
            for device in devices:
                lang = _normalize_language(device["lang"] or "en")
                by_lang.setdefault(lang, []).append(device["push_token"])

            total_sent = 0
            total_failed = 0
            for lang, tokens in by_lang.items():
                title, body = _pick_localized_text(
                    lang,
                    title_translations,
                    body_translations,
                    fallback_title=row["title"],
                    fallback_body=row["body"],
                )
                for batch in _chunk(tokens, EXPO_BATCH_SIZE):
                    sent, failed = _send_expo_batch(batch, title, body)
                    total_sent += sent
                    total_failed += failed

            # ── Finalise ─────────────────────────────────────────────────────
            final_status = "sent" if total_sent > 0 or total_failed == 0 else "failed"
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE api_notification
                    SET status=%s, total_devices=%s, success_count=%s,
                        failure_count=%s, completed_at=%s
                    WHERE id = %s
                    """,
                    (
                        final_status,
                        total_sent + total_failed,
                        total_sent,
                        total_failed,
                        datetime.now(timezone.utc),
                        notification_id,
                    ),
                )
            conn.commit()
            logging.info(
                "Notification %s done: status=%s sent=%s failed=%s",
                notification_id, final_status, total_sent, total_failed,
            )

        except Exception:
            conn.rollback()
            logging.exception("Error processing notification %s", notification_id)
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE api_notification SET status='failed' WHERE id = %s",
                        (notification_id,),
                    )
                conn.commit()
            except Exception:
                logging.exception("Failed to mark notification %s as failed", notification_id)
            raise
        finally:
            conn.close()
