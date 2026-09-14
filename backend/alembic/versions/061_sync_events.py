"""Add sync event cursor primitives

Revision ID: 061
Revises: 060
"""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "061"
down_revision = "060"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "sync_events",
        sa.Column("revision", sa.BigInteger(), sa.Identity(), primary_key=True),
        sa.Column("user_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("entity_type", sa.String(length=50), nullable=False),
        sa.Column("entity_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("book_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("operation", sa.String(length=50), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
    )
    op.create_index("syncEventsUserRevision", "sync_events", ["user_id", "revision"])
    op.create_index("syncEventsBook", "sync_events", ["book_id", "revision"])

    op.add_column(
        "books",
        sa.Column("cover_updated_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.execute(
        "UPDATE books SET cover_updated_at = updated_at WHERE cover_path IS NOT NULL"
    )


def downgrade() -> None:
    op.drop_column("books", "cover_updated_at")
    op.drop_index("syncEventsBook", table_name="sync_events")
    op.drop_index("syncEventsUserRevision", table_name="sync_events")
    op.drop_table("sync_events")
