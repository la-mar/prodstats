"""empty message

Revision ID: 56c3e965f250
Revises: 5becc31ebe92
Create Date: 2020-04-18 16:19:08.100893

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "56c3e965f250"
down_revision = "5becc31ebe92"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column("ip_tests", sa.Column("gas_per10k", sa.Integer(), nullable=True))
    op.add_column("ip_tests", sa.Column("oil_per10k", sa.Integer(), nullable=True))
    op.add_column("ip_tests", sa.Column("water_per10k", sa.Integer(), nullable=True))
    op.create_index(
        "ix_prodstat_api10_prop_agg",
        "prodstats",
        ["api10", "property_name", "aggregate_type"],
        unique=False,
    )
    op.drop_index("prodstat_api10_prop_agg_idx", table_name="prodstats")
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_index(
        "prodstat_api10_prop_agg_idx",
        "prodstats",
        ["api10", "property_name", "aggregate_type"],
        unique=False,
    )
    op.drop_index("ix_prodstat_api10_prop_agg", table_name="prodstats")
    op.drop_column("ip_tests", "water_per10k")
    op.drop_column("ip_tests", "oil_per10k")
    op.drop_column("ip_tests", "gas_per10k")
    # ### end Alembic commands ###
