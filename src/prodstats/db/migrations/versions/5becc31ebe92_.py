"""empty message

Revision ID: 5becc31ebe92
Revises: 9e03472c1f6c
Create Date: 2020-04-18 15:57:22.956252

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "5becc31ebe92"
down_revision = "9e03472c1f6c"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_index(
        "ix_heel_partial",
        "survey_points",
        ["api14", "is_heel_point"],
        unique=False,
        postgresql_where=sa.text("is_heel_point"),
    )
    op.create_index(
        "ix_mid_partial",
        "survey_points",
        ["api14", "is_mid_point"],
        unique=False,
        postgresql_where=sa.text("is_mid_point"),
    )
    op.create_index(
        "ix_toe_partial",
        "survey_points",
        ["api14", "is_toe_point"],
        unique=False,
        postgresql_where=sa.text("is_toe_point"),
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index("ix_toe_partial", table_name="survey_points")
    op.drop_index("ix_mid_partial", table_name="survey_points")
    op.drop_index("ix_heel_partial", table_name="survey_points")
    # ### end Alembic commands ###