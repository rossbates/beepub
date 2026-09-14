import uuid
from datetime import datetime
from typing import Any

from fastapi.encoders import jsonable_encoder
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.reading import Highlight
from app.models.sync import SyncEvent


def record_sync_event(
    db: AsyncSession,
    *,
    operation: str,
    entity_type: str,
    user_id: uuid.UUID | None = None,
    entity_id: uuid.UUID | None = None,
    book_id: uuid.UUID | None = None,
    payload: dict[str, Any] | None = None,
) -> None:
    """Append one durable sync event in the caller's transaction."""
    db.add(
        SyncEvent(
            user_id=user_id,
            entity_type=entity_type,
            entity_id=entity_id,
            book_id=book_id,
            operation=operation,
            payload=jsonable_encoder(payload) if payload is not None else None,
        )
    )


def interaction_payload(**fields: Any) -> dict[str, Any]:
    return jsonable_encoder(fields)


def progress_payload(progress: dict[str, Any]) -> dict[str, Any]:
    return jsonable_encoder(progress)


def highlight_payload(highlight: Highlight) -> dict[str, Any]:
    return jsonable_encoder(
        {
            "id": highlight.id,
            "book_id": highlight.book_id,
            "cfi_range": highlight.cfi_range,
            "text": highlight.text,
            "color": highlight.color,
            "note": highlight.note,
            "prefix": highlight.prefix,
            "suffix": highlight.suffix,
            "section_index": highlight.section_index,
            "created_at": highlight.created_at,
            "updated_at": highlight.updated_at,
            "deleted_at": highlight.deleted_at,
        }
    )


def book_payload(
    *,
    book_id: uuid.UUID,
    updated_at: datetime | None = None,
    cover_updated_at: datetime | None = None,
) -> dict[str, Any]:
    payload: dict[str, uuid.UUID | datetime | None] = {"book_id": book_id}
    if updated_at is not None:
        payload["updated_at"] = updated_at
    if cover_updated_at is not None:
        payload["cover_updated_at"] = cover_updated_at
    return jsonable_encoder(payload)


def bookshelf_payload(
    *, shelf_id: uuid.UUID, name: str | None = None, description: str | None = None
) -> dict[str, Any]:
    payload: dict[str, uuid.UUID | str | None] = {"bookshelf_id": shelf_id}
    if name is not None:
        payload["name"] = name
    if description is not None:
        payload["description"] = description
    return jsonable_encoder(payload)


def shelf_membership_item(
    *,
    book_id: uuid.UUID | None,
    series_key: str | None,
    library_id: uuid.UUID | None,
    sort_order: int,
) -> dict[str, Any]:
    target = "book" if book_id is not None else "series"
    return jsonable_encoder(
        {
            "type": target,
            "book_id": book_id,
            "series_key": series_key,
            "library_id": library_id,
            "sort_order": sort_order,
        }
    )
