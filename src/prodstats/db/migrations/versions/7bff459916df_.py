"""empty message

Revision ID: 7bff459916df
Revises: cc2f89e459b0
Create Date: 2020-04-11 13:34:35.345319

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "7bff459916df"
down_revision = "cc2f89e459b0"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_index(op.f("ix_depths_formation"), "depths", ["formation"], unique=False)
    op.create_index(op.f("ix_depths_grid_id"), "depths", ["grid_id"], unique=False)
    op.create_index(
        op.f("ix_depths_property_name"), "depths", ["property_name"], unique=False
    )
    op.add_column(
        "wells", sa.Column("county_code", sa.String(length=10), nullable=True)
    )
    op.add_column("wells", sa.Column("ground_elev", sa.Integer(), nullable=True))
    op.add_column("wells", sa.Column("hist_operator_alias", sa.String(), nullable=True))
    op.add_column("wells", sa.Column("kb_elev", sa.Integer(), nullable=True))
    op.add_column("wells", sa.Column("operator_alias", sa.String(), nullable=True))
    op.add_column("wells", sa.Column("state_code", sa.String(length=10), nullable=True))
    op.add_column("wells", sa.Column("sub_basin", sa.String(length=50), nullable=True))
    op.create_index(
        op.f("ix_wells_hist_operator_alias"),
        "wells",
        ["hist_operator_alias"],
        unique=False,
    )
    op.create_index(
        op.f("ix_wells_is_producing"), "wells", ["is_producing"], unique=False
    )
    op.create_index(
        op.f("ix_wells_operator_alias"), "wells", ["operator_alias"], unique=False
    )
    op.create_index(op.f("ix_wells_sub_basin"), "wells", ["sub_basin"], unique=False)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f("ix_wells_sub_basin"), table_name="wells")
    op.drop_index(op.f("ix_wells_operator_alias"), table_name="wells")
    op.drop_index(op.f("ix_wells_is_producing"), table_name="wells")
    op.drop_index(op.f("ix_wells_hist_operator_alias"), table_name="wells")
    op.drop_column("wells", "sub_basin")
    op.drop_column("wells", "state_code")
    op.drop_column("wells", "operator_alias")
    op.drop_column("wells", "kb_elev")
    op.drop_column("wells", "hist_operator_alias")
    op.drop_column("wells", "ground_elev")
    op.drop_column("wells", "county_code")
    op.drop_index(op.f("ix_depths_property_name"), table_name="depths")
    op.drop_index(op.f("ix_depths_grid_id"), table_name="depths")
    op.drop_index(op.f("ix_depths_formation"), table_name="depths")
    # ### end Alembic commands ###