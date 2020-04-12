"""empty message

Revision ID: b05ea6419144
Revises: 72c5ff99b815
Create Date: 2020-04-11 17:07:16.772620

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "b05ea6419144"
down_revision = "72c5ff99b815"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column(
        "wellstats",
        sa.Column(
            "comments",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default="{}",
            nullable=False,
        ),
    )
    op.add_column(
        "wellstats",
        sa.Column("date_value", sa.Numeric(precision=19, scale=2), nullable=True),
    )
    op.add_column("wellstats", sa.Column("name", sa.String(length=50), nullable=False))
    op.add_column(
        "wellstats",
        sa.Column("numeric_value", sa.Numeric(precision=19, scale=2), nullable=True),
    )
    op.add_column(
        "wellstats",
        sa.Column("string_value", sa.Numeric(precision=19, scale=2), nullable=True),
    )
    op.add_column("wellstats", sa.Column("type", sa.String(length=25), nullable=False))
    op.drop_column("wellstats", "wellbore_bearing")
    op.drop_column("wellstats", "lateral_dls_roc")
    op.drop_column("wellstats", "wellbore_crow_length")
    op.drop_column("wellstats", "wellbore_direction")
    op.drop_column("wellstats", "wellbore_dls_mc")
    op.drop_column("wellstats", "nearest_deo_api10")
    op.drop_column("wellstats", "wellbore_dls_roc")
    op.drop_column("wellstats", "dist_to_deo_well_mi")
    op.drop_column("wellstats", "nearest_deo_prospect")
    op.drop_column("wellstats", "dist_to_deo_prospect_mi")
    op.drop_column("wellstats", "lateral_dls_mc")
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column(
        "wellstats",
        sa.Column(
            "lateral_dls_mc",
            postgresql.DOUBLE_PRECISION(precision=53),
            autoincrement=False,
            nullable=True,
        ),
    )
    op.add_column(
        "wellstats",
        sa.Column(
            "dist_to_deo_prospect_mi",
            postgresql.DOUBLE_PRECISION(precision=53),
            autoincrement=False,
            nullable=True,
        ),
    )
    op.add_column(
        "wellstats",
        sa.Column(
            "nearest_deo_prospect",
            sa.VARCHAR(length=50),
            autoincrement=False,
            nullable=True,
        ),
    )
    op.add_column(
        "wellstats",
        sa.Column(
            "dist_to_deo_well_mi",
            postgresql.DOUBLE_PRECISION(precision=53),
            autoincrement=False,
            nullable=True,
        ),
    )
    op.add_column(
        "wellstats",
        sa.Column(
            "wellbore_dls_roc",
            postgresql.DOUBLE_PRECISION(precision=53),
            autoincrement=False,
            nullable=True,
        ),
    )
    op.add_column(
        "wellstats",
        sa.Column(
            "nearest_deo_api10",
            sa.VARCHAR(length=50),
            autoincrement=False,
            nullable=True,
        ),
    )
    op.add_column(
        "wellstats",
        sa.Column(
            "wellbore_dls_mc",
            postgresql.DOUBLE_PRECISION(precision=53),
            autoincrement=False,
            nullable=True,
        ),
    )
    op.add_column(
        "wellstats",
        sa.Column(
            "wellbore_direction",
            sa.VARCHAR(length=1),
            autoincrement=False,
            nullable=True,
        ),
    )
    op.add_column(
        "wellstats",
        sa.Column(
            "wellbore_crow_length", sa.INTEGER(), autoincrement=False, nullable=True
        ),
    )
    op.add_column(
        "wellstats",
        sa.Column(
            "lateral_dls_roc",
            postgresql.DOUBLE_PRECISION(precision=53),
            autoincrement=False,
            nullable=True,
        ),
    )
    op.add_column(
        "wellstats",
        sa.Column(
            "wellbore_bearing",
            postgresql.DOUBLE_PRECISION(precision=53),
            autoincrement=False,
            nullable=True,
        ),
    )
    op.drop_column("wellstats", "type")
    op.drop_column("wellstats", "string_value")
    op.drop_column("wellstats", "numeric_value")
    op.drop_column("wellstats", "name")
    op.drop_column("wellstats", "date_value")
    op.drop_column("wellstats", "comments")
    # ### end Alembic commands ###