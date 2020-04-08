"""empty message

Revision ID: 43a898206423
Revises: 8dafa3edecdf
Create Date: 2020-03-25 14:53:50.621102

"""
import geoalchemy2
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "43a898206423"
down_revision = "8dafa3edecdf"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        "surveys",
        sa.Column("api14", sa.String(length=14), nullable=False),
        sa.Column("survey_type", sa.String(length=50), nullable=True),
        sa.Column("survey_method", sa.String(length=50), nullable=True),
        sa.Column("survey_date", sa.Date(), nullable=True),
        sa.Column("depth_top", sa.Integer(), nullable=True),
        sa.Column("depth_top_uom", sa.String(length=10), nullable=True),
        sa.Column("depth_base", sa.Integer(), nullable=True),
        sa.Column("depth_base_uom", sa.String(length=10), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=True,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=True,
        ),
        sa.Column(
            "wellbore",
            geoalchemy2.types.Geometry(geometry_type="LINESTRING", srid=4326),
            nullable=True,
        ),
        sa.Column(
            "lateral_only",
            geoalchemy2.types.Geometry(geometry_type="LINESTRING", srid=4326),
            nullable=True,
        ),
        sa.Column(
            "stick",
            geoalchemy2.types.Geometry(geometry_type="LINESTRING", srid=4326),
            nullable=True,
        ),
        sa.Column(
            "bent_stick",
            geoalchemy2.types.Geometry(geometry_type="LINESTRING", srid=4326),
            nullable=True,
        ),
        sa.Column(
            "wellbore_webmercator",
            geoalchemy2.types.Geometry(geometry_type="LINESTRING", srid=3857),
            nullable=True,
        ),
        sa.Column(
            "lateral_only_webmercator",
            geoalchemy2.types.Geometry(geometry_type="LINESTRING", srid=3857),
            nullable=True,
        ),
        sa.Column(
            "stick_webmercator",
            geoalchemy2.types.Geometry(geometry_type="LINESTRING", srid=3857),
            nullable=True,
        ),
        sa.Column(
            "bent_stick_webmercator",
            geoalchemy2.types.Geometry(geometry_type="LINESTRING", srid=3857),
            nullable=True,
        ),
        sa.PrimaryKeyConstraint("api14"),
    )
    op.create_index(op.f("ix_surveys_api14"), "surveys", ["api14"], unique=False)
    op.create_table(
        "well_locations",
        sa.Column("api14", sa.String(length=14), nullable=False),
        sa.Column("name", sa.String(length=50), nullable=False),
        sa.Column("block", sa.String(length=50), nullable=True),
        sa.Column("section", sa.String(length=50), nullable=True),
        sa.Column("abstract", sa.String(length=50), nullable=True),
        sa.Column("survey", sa.String(length=50), nullable=True),
        sa.Column("metes_bounds", sa.String(length=50), nullable=True),
        sa.Column("lon", sa.Float(), nullable=True),
        sa.Column("lat", sa.Float(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=True,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=True,
        ),
        sa.Column(
            "geom",
            geoalchemy2.types.Geometry(geometry_type="POINT", srid=4326),
            nullable=True,
        ),
        sa.Column(
            "geom_webmercator",
            geoalchemy2.types.Geometry(geometry_type="POINT", srid=3857),
            nullable=True,
        ),
        sa.PrimaryKeyConstraint("api14", "name"),
    )
    op.create_index(
        op.f("ix_well_locations_api14"), "well_locations", ["api14"], unique=False
    )
    op.create_index(
        op.f("ix_well_locations_name"), "well_locations", ["name"], unique=False
    )
    op.drop_index("idx_shapes_bent_stick", table_name="shapes")
    op.drop_index("idx_shapes_bent_stick_webmercator", table_name="shapes")
    op.drop_index("idx_shapes_bhl", table_name="shapes")
    op.drop_index("idx_shapes_bhl_webmercator", table_name="shapes")
    op.drop_index("idx_shapes_heel", table_name="shapes")
    op.drop_index("idx_shapes_heel_webmercator", table_name="shapes")
    op.drop_index("idx_shapes_kop", table_name="shapes")
    op.drop_index("idx_shapes_kop_webmercator", table_name="shapes")
    op.drop_index("idx_shapes_lateral_only", table_name="shapes")
    op.drop_index("idx_shapes_lateral_only_webmercator", table_name="shapes")
    op.drop_index("idx_shapes_mid", table_name="shapes")
    op.drop_index("idx_shapes_mid_webmercator", table_name="shapes")
    op.drop_index("idx_shapes_shl", table_name="shapes")
    op.drop_index("idx_shapes_shl_webmercator", table_name="shapes")
    op.drop_index("idx_shapes_stick", table_name="shapes")
    op.drop_index("idx_shapes_stick_webmercator", table_name="shapes")
    op.drop_index("idx_shapes_toe", table_name="shapes")
    op.drop_index("idx_shapes_toe_webmercator", table_name="shapes")
    op.drop_index("idx_shapes_wellbore", table_name="shapes")
    op.drop_index("idx_shapes_wellbore_webmercator", table_name="shapes")
    op.drop_index("ix_shapes_api14", table_name="shapes")
    op.drop_table("shapes")
    op.add_column(
        "survey_points", sa.Column("is_heel_point", sa.Boolean(), nullable=True)
    )
    op.add_column(
        "survey_points", sa.Column("is_in_lateral", sa.Boolean(), nullable=True)
    )
    op.add_column(
        "survey_points", sa.Column("is_mid_point", sa.Boolean(), nullable=True)
    )
    op.add_column(
        "survey_points", sa.Column("is_toe_point", sa.Boolean(), nullable=True)
    )
    op.add_column("survey_points", sa.Column("sequence", sa.Integer(), nullable=True))
    op.drop_index("idx_survey_points_geom", table_name="survey_points")
    op.drop_index("idx_survey_points_geom_webmercator", table_name="survey_points")
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_index(
        "idx_survey_points_geom_webmercator",
        "survey_points",
        ["geom_webmercator"],
        unique=False,
    )
    op.create_index("idx_survey_points_geom", "survey_points", ["geom"], unique=False)
    op.drop_column("survey_points", "sequence")
    op.drop_column("survey_points", "is_toe_point")
    op.drop_column("survey_points", "is_mid_point")
    op.drop_column("survey_points", "is_in_lateral")
    op.drop_column("survey_points", "is_heel_point")
    op.create_table(
        "shapes",
        sa.Column("api14", sa.VARCHAR(length=14), autoincrement=False, nullable=False),
        sa.Column(
            "shllon",
            postgresql.DOUBLE_PRECISION(precision=53),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column(
            "shllat",
            postgresql.DOUBLE_PRECISION(precision=53),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column(
            "bhllon",
            postgresql.DOUBLE_PRECISION(precision=53),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column(
            "bhllat",
            postgresql.DOUBLE_PRECISION(precision=53),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column(
            "updated_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column(
            "shl",
            geoalchemy2.types.Geometry(geometry_type="POINT", srid=4326),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column(
            "kop",
            geoalchemy2.types.Geometry(geometry_type="POINT", srid=4326),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column(
            "heel",
            geoalchemy2.types.Geometry(geometry_type="POINT", srid=4326),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column(
            "mid",
            geoalchemy2.types.Geometry(geometry_type="POINT", srid=4326),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column(
            "toe",
            geoalchemy2.types.Geometry(geometry_type="POINT", srid=4326),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column(
            "bhl",
            geoalchemy2.types.Geometry(geometry_type="POINT", srid=4326),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column(
            "wellbore",
            geoalchemy2.types.Geometry(geometry_type="LINESTRING", srid=4326),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column(
            "stick",
            geoalchemy2.types.Geometry(geometry_type="LINESTRING", srid=4326),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column(
            "bent_stick",
            geoalchemy2.types.Geometry(geometry_type="LINESTRING", srid=4326),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column(
            "lateral_only",
            geoalchemy2.types.Geometry(geometry_type="LINESTRING", srid=4326),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column(
            "shl_webmercator",
            geoalchemy2.types.Geometry(geometry_type="POINT", srid=3857),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column(
            "kop_webmercator",
            geoalchemy2.types.Geometry(geometry_type="POINT", srid=3857),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column(
            "heel_webmercator",
            geoalchemy2.types.Geometry(geometry_type="POINT", srid=3857),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column(
            "mid_webmercator",
            geoalchemy2.types.Geometry(geometry_type="POINT", srid=3857),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column(
            "toe_webmercator",
            geoalchemy2.types.Geometry(geometry_type="POINT", srid=3857),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column(
            "bhl_webmercator",
            geoalchemy2.types.Geometry(geometry_type="POINT", srid=3857),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column(
            "wellbore_webmercator",
            geoalchemy2.types.Geometry(geometry_type="LINESTRING", srid=3857),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column(
            "stick_webmercator",
            geoalchemy2.types.Geometry(geometry_type="LINESTRING", srid=3857),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column(
            "bent_stick_webmercator",
            geoalchemy2.types.Geometry(geometry_type="LINESTRING", srid=3857),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column(
            "lateral_only_webmercator",
            geoalchemy2.types.Geometry(geometry_type="LINESTRING", srid=3857),
            autoincrement=False,
            nullable=True,
        ),
        sa.PrimaryKeyConstraint("api14", name="shapes_pkey"),
    )
    op.create_index("ix_shapes_api14", "shapes", ["api14"], unique=False)
    op.create_index(
        "idx_shapes_wellbore_webmercator",
        "shapes",
        ["wellbore_webmercator"],
        unique=False,
    )
    op.create_index("idx_shapes_wellbore", "shapes", ["wellbore"], unique=False)
    op.create_index(
        "idx_shapes_toe_webmercator", "shapes", ["toe_webmercator"], unique=False
    )
    op.create_index("idx_shapes_toe", "shapes", ["toe"], unique=False)
    op.create_index(
        "idx_shapes_stick_webmercator", "shapes", ["stick_webmercator"], unique=False
    )
    op.create_index("idx_shapes_stick", "shapes", ["stick"], unique=False)
    op.create_index(
        "idx_shapes_shl_webmercator", "shapes", ["shl_webmercator"], unique=False
    )
    op.create_index("idx_shapes_shl", "shapes", ["shl"], unique=False)
    op.create_index(
        "idx_shapes_mid_webmercator", "shapes", ["mid_webmercator"], unique=False
    )
    op.create_index("idx_shapes_mid", "shapes", ["mid"], unique=False)
    op.create_index(
        "idx_shapes_lateral_only_webmercator",
        "shapes",
        ["lateral_only_webmercator"],
        unique=False,
    )
    op.create_index("idx_shapes_lateral_only", "shapes", ["lateral_only"], unique=False)
    op.create_index(
        "idx_shapes_kop_webmercator", "shapes", ["kop_webmercator"], unique=False
    )
    op.create_index("idx_shapes_kop", "shapes", ["kop"], unique=False)
    op.create_index(
        "idx_shapes_heel_webmercator", "shapes", ["heel_webmercator"], unique=False
    )
    op.create_index("idx_shapes_heel", "shapes", ["heel"], unique=False)
    op.create_index(
        "idx_shapes_bhl_webmercator", "shapes", ["bhl_webmercator"], unique=False
    )
    op.create_index("idx_shapes_bhl", "shapes", ["bhl"], unique=False)
    op.create_index(
        "idx_shapes_bent_stick_webmercator",
        "shapes",
        ["bent_stick_webmercator"],
        unique=False,
    )
    op.create_index("idx_shapes_bent_stick", "shapes", ["bent_stick"], unique=False)
    op.drop_index(op.f("ix_well_locations_name"), table_name="well_locations")
    op.drop_index(op.f("ix_well_locations_api14"), table_name="well_locations")
    op.drop_table("well_locations")
    op.drop_index(op.f("ix_surveys_api14"), table_name="surveys")
    op.drop_table("surveys")
    # ### end Alembic commands ###