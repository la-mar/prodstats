""" initial

Revision ID: 244869cf6945
Revises:
Create Date: 2020-03-05 18:59:05.382421

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "244869cf6945"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.execute("create extension if not exists postgis;")


def downgrade():
    pass
    # op.execute("drop extension if exists postgis;")
