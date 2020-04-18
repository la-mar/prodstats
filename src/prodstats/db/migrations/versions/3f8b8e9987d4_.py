"""empty message

Revision ID: 3f8b8e9987d4
Revises: d93f5866d24b
Create Date: 2020-04-18 15:53:47.304649

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "3f8b8e9987d4"
down_revision = "d93f5866d24b"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column(
        "survey_points", "is_hard_corner", existing_type=sa.BOOLEAN(), nullable=False
    )
    op.alter_column(
        "survey_points", "is_heel_point", existing_type=sa.BOOLEAN(), nullable=False
    )
    op.alter_column(
        "survey_points", "is_in_lateral", existing_type=sa.BOOLEAN(), nullable=False
    )
    op.alter_column(
        "survey_points", "is_kop", existing_type=sa.BOOLEAN(), nullable=False
    )
    op.alter_column(
        "survey_points", "is_mid_point", existing_type=sa.BOOLEAN(), nullable=False
    )
    op.alter_column(
        "survey_points", "is_soft_corner", existing_type=sa.BOOLEAN(), nullable=False
    )
    op.alter_column(
        "survey_points", "is_toe_point", existing_type=sa.BOOLEAN(), nullable=False
    )
    op.add_column("wells", sa.Column("well_name", sa.String(), nullable=True))
    op.create_index(
        "ix_well_basin_holedir_isprod",
        "wells",
        ["basin", "hole_direction", "is_producing"],
        unique=False,
    )
    op.create_index("ix_well_basin_status", "wells", ["basin", "status"], unique=False)
    op.drop_index("well_basin_holedir_isprod_idx", table_name="wells")
    op.drop_index("well_basin_status_idx", table_name="wells")
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_index("well_basin_status_idx", "wells", ["basin", "status"], unique=False)
    op.create_index(
        "well_basin_holedir_isprod_idx",
        "wells",
        ["basin", "hole_direction", "is_producing"],
        unique=False,
    )
    op.drop_index("ix_well_basin_status", table_name="wells")
    op.drop_index("ix_well_basin_holedir_isprod", table_name="wells")
    op.drop_column("wells", "well_name")
    op.alter_column(
        "survey_points", "is_toe_point", existing_type=sa.BOOLEAN(), nullable=True
    )
    op.alter_column(
        "survey_points", "is_soft_corner", existing_type=sa.BOOLEAN(), nullable=True
    )
    op.alter_column(
        "survey_points", "is_mid_point", existing_type=sa.BOOLEAN(), nullable=True
    )
    op.alter_column(
        "survey_points", "is_kop", existing_type=sa.BOOLEAN(), nullable=True
    )
    op.alter_column(
        "survey_points", "is_in_lateral", existing_type=sa.BOOLEAN(), nullable=True
    )
    op.alter_column(
        "survey_points", "is_heel_point", existing_type=sa.BOOLEAN(), nullable=True
    )
    op.alter_column(
        "survey_points", "is_hard_corner", existing_type=sa.BOOLEAN(), nullable=True
    )
    # ### end Alembic commands ###
