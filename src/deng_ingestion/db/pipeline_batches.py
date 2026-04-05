from __future__ import annotations

from psycopg2.extensions import connection as PgConnection

CLAIM_TTL_MINUTES = 30


def truncate_error_message(message: str, max_length: int = 1000) -> str:
    if len(message) <= max_length:
        return message
    return message[: max_length - 3] + "..."


def mark_batch_downloaded(conn: PgConnection, batch_id: int) -> None:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            UPDATE pipeline_batches
            SET
                status = 'downloaded',
                downloaded_at = COALESCE(downloaded_at, NOW()),
                error_message = NULL
            WHERE batch_id = %(batch_id)s
            """,
            {"batch_id": batch_id},
        )


def mark_batch_loaded(conn: PgConnection, batch_id: int) -> None:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            UPDATE pipeline_batches
            SET
                status = 'loaded',
                loaded_at = NOW(),
                claimed_at = NULL,
                claimed_by = NULL,
                error_message = NULL
            WHERE batch_id = %(batch_id)s
            """,
            {"batch_id": batch_id},
        )


def clear_batch_claim(conn: PgConnection, batch_id: int) -> None:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            UPDATE pipeline_batches
            SET
                claimed_at = NULL,
                claimed_by = NULL,
                error_message = NULL
            WHERE batch_id = %(batch_id)s
            """,
            {"batch_id": batch_id},
        )


def mark_batch_failed(
    conn: PgConnection,
    batch_id: int,
    error: Exception | str,
) -> None:
    error_message = (
        truncate_error_message(f"{type(error).__name__}: {error}")
        if isinstance(error, Exception)
        else truncate_error_message(error)
    )

    with conn.cursor() as cursor:
        cursor.execute(
            """
            UPDATE pipeline_batches
            SET
                status = 'failed',
                claimed_at = NULL,
                claimed_by = NULL,
                error_message = %(error_message)s
            WHERE batch_id = %(batch_id)s
            """,
            {
                "batch_id": batch_id,
                "error_message": error_message,
            },
        )
