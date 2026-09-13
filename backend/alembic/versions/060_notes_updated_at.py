"""Add LWW anchor for book notes

Book-level notes were editable through the web API but had no per-group
sync stamp, which made native/offline two-way sync guess at conflict order.
Give notes the same treatment as rating/favorite/status: web writes stamp
server-now, sync clients send their own notes_updated_at.

Revision ID: 060
Revises: 059
"""

import sqlalchemy as sa

from alembic import op

revision = "060"
down_revision = "059"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "user_book_interactions",
        sa.Column("notes_updated_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.execute(
        "UPDATE user_book_interactions SET notes_updated_at = updated_at"
        " WHERE notes IS NOT NULL"
    )


def downgrade() -> None:
    op.drop_column("user_book_interactions", "notes_updated_at")
