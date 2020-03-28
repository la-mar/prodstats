"""empty message

Revision ID: 124e44d9264c
Revises: 43a898206423
Create Date: 2020-03-28 09:54:52.080248

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "124e44d9264c"
down_revision = "43a898206423"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column(
        "survey_points", sa.Column("is_hard_corner", sa.Boolean(), nullable=True)
    )
    op.add_column("survey_points", sa.Column("is_kop", sa.Boolean(), nullable=True))
    op.add_column(
        "survey_points", sa.Column("is_soft_corner", sa.Boolean(), nullable=True)
    )
    op.add_column("survey_points", sa.Column("theta", sa.Float(), nullable=True))
    op.drop_index("idx_surveys_bent_stick", table_name="surveys")
    op.drop_index("idx_surveys_bent_stick_webmercator", table_name="surveys")
    op.drop_index("idx_surveys_lateral_only", table_name="surveys")
    op.drop_index("idx_surveys_lateral_only_webmercator", table_name="surveys")
    op.drop_index("idx_surveys_stick", table_name="surveys")
    op.drop_index("idx_surveys_stick_webmercator", table_name="surveys")
    op.drop_index("idx_surveys_wellbore", table_name="surveys")
    op.drop_index("idx_surveys_wellbore_webmercator", table_name="surveys")
    op.drop_index("idx_well_locations_geom", table_name="well_locations")
    op.drop_index("idx_well_locations_geom_webmercator", table_name="well_locations")
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_index(
        "idx_well_locations_geom_webmercator",
        "well_locations",
        ["geom_webmercator"],
        unique=False,
    )
    op.create_index("idx_well_locations_geom", "well_locations", ["geom"], unique=False)
    op.create_index(
        "idx_surveys_wellbore_webmercator",
        "surveys",
        ["wellbore_webmercator"],
        unique=False,
    )
    op.create_index("idx_surveys_wellbore", "surveys", ["wellbore"], unique=False)
    op.create_index(
        "idx_surveys_stick_webmercator", "surveys", ["stick_webmercator"], unique=False
    )
    op.create_index("idx_surveys_stick", "surveys", ["stick"], unique=False)
    op.create_index(
        "idx_surveys_lateral_only_webmercator",
        "surveys",
        ["lateral_only_webmercator"],
        unique=False,
    )
    op.create_index(
        "idx_surveys_lateral_only", "surveys", ["lateral_only"], unique=False
    )
    op.create_index(
        "idx_surveys_bent_stick_webmercator",
        "surveys",
        ["bent_stick_webmercator"],
        unique=False,
    )
    op.create_index("idx_surveys_bent_stick", "surveys", ["bent_stick"], unique=False)
    op.drop_column("survey_points", "theta")
    op.drop_column("survey_points", "is_soft_corner")
    op.drop_column("survey_points", "is_kop")
    op.drop_column("survey_points", "is_hard_corner")
    # ### end Alembic commands ###
