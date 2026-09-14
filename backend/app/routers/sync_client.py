"""Native/offline-client sync discovery and cursor pull.

Mutating through the cursor endpoint comes later; this phase gives clients a
monotonic remote-change stream so they can stop polling every book like some
bureaucratic candlelighter.
"""

import uuid
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel, Field, model_validator
from sqlalchemy import and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.deps import get_current_user
from app.models.book import Book
from app.models.bookshelf import Bookshelf, BookshelfBook
from app.models.library import Library, LibraryBook
from app.models.reading import Highlight, UserBookInteraction
from app.models.sync import SyncEvent
from app.models.user import User, UserRole
from app.routers.libraries import accessible_book_ids_select
from app.schemas.book import BookOut
from app.services.sync_events import highlight_payload, shelf_membership_item

router = APIRouter(prefix="/api/sync", tags=["client-sync"])

MAX_MUTATIONS = 500
MAX_CHANGES = 1000
SYNC_FEATURES = [
    "cursor",
    "catalogue",
    "shelves",
    "highlights",
    "progress",
    "interaction",
    "notes",
    "activity",
]
_SNAPSHOT_PREFIX = "snapshot:"


class SyncCapabilitiesOut(BaseModel):
    endpoint: str
    features: list[str]
    max_mutations: int
    max_changes: int


class SyncClientMutation(BaseModel):
    id: str | None = None
    type: str
    payload: dict[str, Any] = Field(default_factory=dict)


class SyncClientRequest(BaseModel):
    client_id: str = Field(min_length=1, max_length=128)
    cursor: str | None = None
    max_changes: int = Field(default=MAX_CHANGES, ge=1, le=MAX_CHANGES)
    mutations: list[SyncClientMutation] = Field(
        default_factory=list, max_length=MAX_MUTATIONS
    )

    @model_validator(mode="after")
    def pull_only_for_now(self) -> "SyncClientRequest":
        if self.mutations:
            raise ValueError("/api/sync/client is pull-only for now")
        return self


class SyncClientChange(BaseModel):
    revision: str
    type: str
    book_id: uuid.UUID | None = None
    entity_id: uuid.UUID | None = None
    payload: dict[str, Any] | None = None


class SyncClientAck(BaseModel):
    id: str
    status: str
    error: str | None = None


class SyncClientResponse(BaseModel):
    cursor: str
    has_more: bool
    acks: list[SyncClientAck] = Field(default_factory=list)
    changes: list[SyncClientChange]


@router.get("/capabilities", response_model=SyncCapabilitiesOut)
async def sync_capabilities(
    current_user: Annotated[User, Depends(get_current_user)],
) -> SyncCapabilitiesOut:
    """Describe the native sync API available to the authenticated user."""
    _ = current_user
    return SyncCapabilitiesOut(
        endpoint="/api/sync/client",
        features=SYNC_FEATURES,
        max_mutations=MAX_MUTATIONS,
        max_changes=MAX_CHANGES,
    )


@router.post("/client", response_model=SyncClientResponse)
async def sync_client(
    body: SyncClientRequest,
    current_user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
) -> SyncClientResponse:
    """Pull remote changes after a cursor.

    ``cursor = null`` bootstraps a synthetic snapshot of the user's accessible
    library. Once the snapshot has been fully paged, the cursor becomes the
    numeric high-water ``sync_events.revision`` captured at snapshot start.
    """
    _ = body.client_id
    if body.cursor is None:
        highwater = await _current_highwater(db)
        return await _snapshot_page(
            db,
            user=current_user,
            start=0,
            limit=body.max_changes,
            highwater=highwater,
        )
    if body.cursor.startswith(_SNAPSHOT_PREFIX):
        start, highwater = _parse_snapshot_cursor(body.cursor)
        return await _snapshot_page(
            db,
            user=current_user,
            start=start,
            limit=body.max_changes,
            highwater=highwater,
        )
    try:
        cursor = int(body.cursor)
    except ValueError:
        raise HTTPException(status_code=422, detail="Invalid sync cursor")
    if cursor < 0:
        raise HTTPException(status_code=422, detail="Invalid sync cursor")
    return await _event_page(
        db,
        user=current_user,
        cursor=cursor,
        limit=body.max_changes,
        highwater=await _current_highwater(db),
    )


def _parse_snapshot_cursor(cursor: str) -> tuple[int, int]:
    parts = cursor.split(":", 2)
    if len(parts) != 3 or parts[0] != "snapshot":
        raise HTTPException(status_code=422, detail="Invalid sync cursor")
    try:
        start = int(parts[1])
        highwater = int(parts[2])
    except ValueError:
        raise HTTPException(status_code=422, detail="Invalid sync cursor")
    if start < 0 or highwater < 0:
        raise HTTPException(status_code=422, detail="Invalid sync cursor")
    return start, highwater


async def _current_highwater(db: AsyncSession) -> int:
    return int(await db.scalar(select(func.coalesce(func.max(SyncEvent.revision), 0))))


async def _event_page(
    db: AsyncSession,
    *,
    user: User,
    cursor: int,
    limit: int,
    highwater: int,
) -> SyncClientResponse:
    result = await db.execute(
        select(SyncEvent)
        .where(SyncEvent.revision > cursor, SyncEvent.revision <= highwater)
        .where(_visible_event_condition(user))
        .order_by(SyncEvent.revision.asc())
        .limit(limit + 1)
    )
    events = list(result.scalars().all())
    has_more = len(events) > limit
    page = events[:limit]
    changes = [
        SyncClientChange(
            revision=str(event.revision),
            type=event.operation,
            book_id=event.book_id,
            entity_id=event.entity_id,
            payload=event.payload,
        )
        for event in page
    ]
    next_cursor = page[-1].revision if page else highwater
    return SyncClientResponse(
        cursor=str(next_cursor),
        has_more=has_more,
        changes=changes,
    )


def _visible_event_condition(user: User):
    if user.role == UserRole.admin:
        return or_(SyncEvent.user_id == user.id, SyncEvent.user_id.is_(None))
    accessible_books = accessible_book_ids_select(user)
    return or_(
        SyncEvent.user_id == user.id,
        and_(
            SyncEvent.user_id.is_(None),
            or_(
                SyncEvent.book_id.is_(None),
                SyncEvent.book_id.in_(accessible_books),
                SyncEvent.operation == "book_delete",
            ),
        ),
    )


async def _snapshot_page(
    db: AsyncSession,
    *,
    user: User,
    start: int,
    limit: int,
    highwater: int,
) -> SyncClientResponse:
    changes: list[SyncClientChange] = []
    category_start = 0

    for count_fn, append_fn in (
        (_book_count, _append_book_snapshot),
        (_interaction_count, _append_interaction_snapshot),
        (_highlight_count, _append_highlight_snapshot),
        (_bookshelf_count, _append_bookshelf_snapshot),
        (_membership_count, _append_membership_snapshot),
    ):
        count = await count_fn(db, user)
        category_end = category_start + count
        if start < category_end and len(changes) < limit:
            await append_fn(
                db,
                user=user,
                offset=max(0, start - category_start),
                limit=limit,
                changes=changes,
            )
        category_start = category_end

    next_offset = start + len(changes)
    has_more = category_start > next_offset
    cursor = (
        f"{_SNAPSHOT_PREFIX}{next_offset}:{highwater}" if has_more else str(highwater)
    )
    return SyncClientResponse(cursor=cursor, has_more=has_more, changes=changes)


async def _append_book_snapshot(
    db: AsyncSession,
    *,
    user: User,
    offset: int,
    limit: int,
    changes: list[SyncClientChange],
) -> int:
    count = await _book_count(db, user)
    if offset >= count:
        return offset - count
    remaining = limit - len(changes)
    if remaining <= 0:
        return offset
    result = await db.execute(
        select(Book, Library.id, Library.name)
        .join(LibraryBook, LibraryBook.book_id == Book.id)
        .join(Library, Library.id == LibraryBook.library_id)
        .where(Book.id.in_(accessible_book_ids_select(user)))
        .order_by(Book.created_at.asc(), Book.id.asc())
        .offset(offset)
        .limit(remaining)
    )
    rows = result.all()
    for index, (book, library_id, library_name) in enumerate(rows, start=offset + 1):
        payload = BookOut.model_validate(book)
        payload.library_id = library_id
        payload.library_names = [library_name]
        changes.append(
            SyncClientChange(
                revision=f"snapshot:book:{index}",
                type="book_upsert",
                book_id=book.id,
                entity_id=book.id,
                payload=jsonable_encoder(payload),
            )
        )
    return offset + len(rows)


async def _append_interaction_snapshot(
    db: AsyncSession,
    *,
    user: User,
    offset: int,
    limit: int,
    changes: list[SyncClientChange],
) -> int:
    count = await _interaction_count(db, user)
    if offset >= count:
        return offset - count
    remaining = limit - len(changes)
    if remaining <= 0:
        return offset
    result = await db.execute(
        select(UserBookInteraction)
        .where(
            UserBookInteraction.user_id == user.id,
            UserBookInteraction.book_id.in_(accessible_book_ids_select(user)),
        )
        .order_by(
            UserBookInteraction.updated_at.asc(), UserBookInteraction.book_id.asc()
        )
        .offset(offset)
        .limit(remaining)
    )
    rows = list(result.scalars().all())
    for index, interaction in enumerate(rows, start=offset + 1):
        changes.append(
            SyncClientChange(
                revision=f"snapshot:interaction:{index}",
                type="interaction_update",
                book_id=interaction.book_id,
                entity_id=interaction.book_id,
                payload=_interaction_snapshot_payload(interaction),
            )
        )
    return offset + len(rows)


async def _append_highlight_snapshot(
    db: AsyncSession,
    *,
    user: User,
    offset: int,
    limit: int,
    changes: list[SyncClientChange],
) -> int:
    count = await _highlight_count(db, user)
    if offset >= count:
        return offset - count
    remaining = limit - len(changes)
    if remaining <= 0:
        return offset
    result = await db.execute(
        select(Highlight)
        .where(
            Highlight.user_id == user.id,
            Highlight.book_id.in_(accessible_book_ids_select(user)),
        )
        .order_by(Highlight.created_at.asc(), Highlight.id.asc())
        .offset(offset)
        .limit(remaining)
    )
    rows = list(result.scalars().all())
    for index, highlight in enumerate(rows, start=offset + 1):
        changes.append(
            SyncClientChange(
                revision=f"snapshot:highlight:{index}",
                type=(
                    "highlight_delete" if highlight.deleted_at else "highlight_upsert"
                ),
                book_id=highlight.book_id,
                entity_id=highlight.id,
                payload=highlight_payload(highlight),
            )
        )
    return offset + len(rows)


async def _append_bookshelf_snapshot(
    db: AsyncSession,
    *,
    user: User,
    offset: int,
    limit: int,
    changes: list[SyncClientChange],
) -> int:
    count = await _bookshelf_count(db, user)
    if offset >= count:
        return offset - count
    remaining = limit - len(changes)
    if remaining <= 0:
        return offset
    result = await db.execute(
        select(Bookshelf)
        .where(Bookshelf.user_id == user.id)
        .order_by(Bookshelf.created_at.asc(), Bookshelf.id.asc())
        .offset(offset)
        .limit(remaining)
    )
    rows = list(result.scalars().all())
    for index, shelf in enumerate(rows, start=offset + 1):
        changes.append(
            SyncClientChange(
                revision=f"snapshot:bookshelf:{index}",
                type="bookshelf_upsert",
                entity_id=shelf.id,
                payload=jsonable_encoder(
                    {
                        "bookshelf_id": shelf.id,
                        "name": shelf.name,
                        "description": shelf.description,
                        "created_at": shelf.created_at,
                        "updated_at": shelf.updated_at,
                    }
                ),
            )
        )
    return offset + len(rows)


async def _append_membership_snapshot(
    db: AsyncSession,
    *,
    user: User,
    offset: int,
    limit: int,
    changes: list[SyncClientChange],
) -> int:
    count = await _membership_count(db, user)
    if offset >= count:
        return offset - count
    remaining = limit - len(changes)
    if remaining <= 0:
        return offset
    shelves = list(
        (
            await db.execute(
                select(Bookshelf)
                .where(Bookshelf.user_id == user.id)
                .order_by(Bookshelf.created_at.asc(), Bookshelf.id.asc())
                .offset(offset)
                .limit(remaining)
            )
        )
        .scalars()
        .all()
    )
    for index, shelf in enumerate(shelves, start=offset + 1):
        result = await db.execute(
            select(BookshelfBook)
            .where(BookshelfBook.bookshelf_id == shelf.id)
            .order_by(BookshelfBook.sort_order.asc())
        )
        items = [
            shelf_membership_item(
                book_id=row.book_id,
                series_key=row.series_key,
                library_id=row.library_id,
                sort_order=row.sort_order,
            )
            for row in result.scalars().all()
        ]
        changes.append(
            SyncClientChange(
                revision=f"snapshot:bookshelf_membership:{index}",
                type="bookshelf_membership_update",
                entity_id=shelf.id,
                payload=jsonable_encoder({"bookshelf_id": shelf.id, "items": items}),
            )
        )
    return offset + len(shelves)


def _interaction_snapshot_payload(interaction: UserBookInteraction) -> dict[str, Any]:
    return jsonable_encoder(
        {
            "book_id": interaction.book_id,
            "reading_status": interaction.reading_status,
            "started_at": interaction.started_at,
            "finished_at": interaction.finished_at,
            "status_updated_at": interaction.status_updated_at,
            "rating": interaction.rating,
            "rating_updated_at": interaction.rating_updated_at,
            "is_favorite": interaction.is_favorite,
            "favorite_updated_at": interaction.favorite_updated_at,
            "notes": interaction.notes,
            "notes_updated_at": interaction.notes_updated_at,
            "reading_progress": interaction.reading_progress,
            "updated_at": interaction.updated_at,
        }
    )


async def _book_count(db: AsyncSession, user: User) -> int:
    return int(
        await db.scalar(
            select(func.count())
            .select_from(Book)
            .where(Book.id.in_(accessible_book_ids_select(user)))
        )
        or 0
    )


async def _interaction_count(db: AsyncSession, user: User) -> int:
    return int(
        await db.scalar(
            select(func.count())
            .select_from(UserBookInteraction)
            .where(
                UserBookInteraction.user_id == user.id,
                UserBookInteraction.book_id.in_(accessible_book_ids_select(user)),
            )
        )
        or 0
    )


async def _highlight_count(db: AsyncSession, user: User) -> int:
    return int(
        await db.scalar(
            select(func.count())
            .select_from(Highlight)
            .where(
                Highlight.user_id == user.id,
                Highlight.book_id.in_(accessible_book_ids_select(user)),
            )
        )
        or 0
    )


async def _bookshelf_count(db: AsyncSession, user: User) -> int:
    return int(
        await db.scalar(
            select(func.count())
            .select_from(Bookshelf)
            .where(Bookshelf.user_id == user.id)
        )
        or 0
    )


async def _membership_count(db: AsyncSession, user: User) -> int:
    # One coarse membership snapshot per shelf.
    return await _bookshelf_count(db, user)
