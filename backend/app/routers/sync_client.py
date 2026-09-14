"""Native/offline-client sync discovery.

The actual cursor endpoint comes next; this advertises the contract so clients
can switch deliberately instead of probing URLs like a raccoon in a pantry.
"""

from typing import Annotated

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from app.deps import get_current_user
from app.models.user import User

router = APIRouter(prefix="/api/sync", tags=["client-sync"])

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


class SyncCapabilitiesOut(BaseModel):
    endpoint: str
    features: list[str]
    max_mutations: int
    max_changes: int


@router.get("/capabilities", response_model=SyncCapabilitiesOut)
async def sync_capabilities(
    current_user: Annotated[User, Depends(get_current_user)],
) -> SyncCapabilitiesOut:
    """Describe the native sync API available to the authenticated user."""
    _ = current_user
    return SyncCapabilitiesOut(
        endpoint="/api/sync/client",
        features=SYNC_FEATURES,
        max_mutations=500,
        max_changes=1000,
    )
