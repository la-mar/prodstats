"""first major

Revision ID: 03d10d7b15e7
Revises: 244869cf6945
Create Date: 2020-03-20 18:48:38.264950

"""
import geoalchemy2
import sqlalchemy as sa
import sqlalchemy_utils
from alembic import op
from sqlalchemy.dialects import postgresql

import const

# revision identifiers, used by Alembic.
revision = "03d10d7b15e7"
down_revision = "244869cf6945"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        "areas",
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
        sa.Column("area", sa.String(length=25), nullable=False),
        sa.Column(
            "type",
            sqlalchemy_utils.types.choice.ChoiceType(const.EntityType),
            nullable=False,
        ),
        sa.Column(
            "hole_direction",
            sqlalchemy_utils.types.choice.ChoiceType(const.HoleDirection),
            nullable=False,
        ),
        sa.Column(
            "path",
            sqlalchemy_utils.types.choice.ChoiceType(const.IHSPath),
            nullable=True,
        ),
        sa.Column("last_sync", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("area", "type", "hole_direction"),
    )
    op.create_table(
        "depths",
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
        sa.Column("api14", sa.String(length=14), nullable=False),
        sa.Column("name", sa.String(length=50), nullable=False),
        sa.Column("value", sa.Integer(), nullable=True),
        sa.Column("grid_id", sa.Integer(), nullable=True),
        sa.Column("formation", sa.String(length=50), nullable=True),
        sa.Column("into_formation_feet", sa.Integer(), nullable=True),
        sa.Column("into_formation_percent", sa.Float(), nullable=True),
        sa.Column("above_next_formation_feet", sa.Integer(), nullable=True),
        sa.Column("above_next_formation_percent", sa.Float(), nullable=True),
        sa.Column("overlap_feet", sa.Integer(), nullable=True),
        sa.Column("overlap_percent", sa.Float(), nullable=True),
        sa.Column("in_target", sa.Boolean(), nullable=True),
        sa.PrimaryKeyConstraint("api14", "name"),
    )
    op.create_index(op.f("ix_depths_api14"), "depths", ["api14"], unique=False)
    op.create_index(op.f("ix_depths_name"), "depths", ["name"], unique=False)
    op.create_table(
        "prodstats",
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
        sa.Column("api10", sa.String(length=10), nullable=False),
        sa.Column("name", sa.String(length=50), nullable=False),
        sa.Column("value", sa.Numeric(precision=19, scale=2), nullable=True),
        sa.Column("property_name", sa.String(length=50), nullable=True),
        sa.Column("aggregate_type", sa.String(length=25), nullable=True),
        sa.Column("is_peak_norm", sa.Boolean(), nullable=True),
        sa.Column("is_ll_norm", sa.Boolean(), nullable=True),
        sa.Column("ll_norm_value", sa.Integer(), nullable=True),
        sa.Column("includes_zeroes", sa.Boolean(), nullable=True),
        sa.Column("start_date", sa.Date(), nullable=True),
        sa.Column("end_date", sa.Date(), nullable=True),
        sa.Column("start_month", sa.Integer(), nullable=True),
        sa.Column("end_month", sa.Integer(), nullable=True),
        sa.Column(
            "comments",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default="{}",
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("api10", "name"),
    )
    op.create_table(
        "production_header",
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
        sa.Column("api10", sa.String(length=10), nullable=False),
        sa.Column("primary_api14", sa.String(length=14), nullable=True),
        sa.Column("entity12", sa.String(length=12), nullable=False),
        sa.Column("status", sa.String(), nullable=True),
        sa.Column("first_prod_date", sa.Date(), nullable=True),
        sa.Column("last_prod_date", sa.Date(), nullable=True),
        sa.Column("prod_months", sa.Integer(), nullable=True),
        sa.Column("prod_days", sa.Integer(), nullable=True),
        sa.Column("peak_norm_months", sa.Integer(), nullable=True),
        sa.Column("peak_norm_days", sa.Integer(), nullable=True),
        sa.Column("peak30_oil", sa.Integer(), nullable=True),
        sa.Column("peak30_gas", sa.Integer(), nullable=True),
        sa.Column("peak30_date", sa.Date(), nullable=True),
        sa.Column("peak30_month", sa.Integer(), nullable=True),
        sa.Column("perfll", sa.Integer(), nullable=True),
        sa.Column("perf_upper", sa.Integer(), nullable=True),
        sa.Column("perf_lower", sa.Integer(), nullable=True),
        sa.Column("oil_pdp_last3mo_per30Kbbl", sa.Integer(), nullable=True),
        sa.Column("boe_pdp_last3mo_per30Kbbl", sa.Integer(), nullable=True),
        sa.Column("products", sa.String(), nullable=True),
        sa.Column("provider", sa.String(), nullable=True),
        sa.Column("provider_last_update_at", sa.DateTime(), nullable=True),
        sa.Column(
            "related_wells",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default="[]",
            nullable=False,
        ),
        sa.Column(
            "comments",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default="[]",
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("api10"),
    )
    op.create_table(
        "production_monthly",
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
        sa.Column("api10", sa.String(length=10), nullable=False),
        sa.Column("prod_date", sa.Date(), nullable=False),
        sa.Column("prod_month", sa.Integer(), nullable=True),
        sa.Column("days_in_month", sa.Integer(), nullable=True),
        sa.Column("prod_days", sa.Integer(), nullable=True),
        sa.Column("peak_norm_month", sa.Integer(), nullable=True),
        sa.Column("peak_norm_days", sa.Integer(), nullable=True),
        sa.Column("oil", sa.Integer(), nullable=True),
        sa.Column("gas", sa.Integer(), nullable=True),
        sa.Column("water", sa.Integer(), nullable=True),
        sa.Column("boe", sa.Integer(), nullable=True),
        sa.Column("water_cut", sa.Numeric(precision=19, scale=2), nullable=True),
        sa.Column("oil_percent", sa.Numeric(precision=19, scale=2), nullable=True),
        sa.Column("gor", sa.Integer(), nullable=True),
        sa.Column("oil_norm_1k", sa.Integer(), nullable=True),
        sa.Column("gas_norm_1k", sa.Integer(), nullable=True),
        sa.Column("water_norm_1k", sa.Integer(), nullable=True),
        sa.Column("boe_norm_1k", sa.Integer(), nullable=True),
        sa.Column("gas_norm_3k", sa.Integer(), nullable=True),
        sa.Column("oil_norm_3k", sa.Integer(), nullable=True),
        sa.Column("water_norm_3k", sa.Integer(), nullable=True),
        sa.Column("boe_norm_3k", sa.Integer(), nullable=True),
        sa.Column("gas_norm_5k", sa.Integer(), nullable=True),
        sa.Column("oil_norm_5k", sa.Integer(), nullable=True),
        sa.Column("water_norm_5k", sa.Integer(), nullable=True),
        sa.Column("boe_norm_5k", sa.Integer(), nullable=True),
        sa.Column("gas_norm_7500", sa.Integer(), nullable=True),
        sa.Column("oil_norm_7500", sa.Integer(), nullable=True),
        sa.Column("water_norm_7500", sa.Integer(), nullable=True),
        sa.Column("boe_norm_7500", sa.Integer(), nullable=True),
        sa.Column("gas_norm_10k", sa.Integer(), nullable=True),
        sa.Column("oil_norm_10k", sa.Integer(), nullable=True),
        sa.Column("water_norm_10k", sa.Integer(), nullable=True),
        sa.Column("boe_norm_10k", sa.Integer(), nullable=True),
        sa.Column("oil_avg_daily", sa.Integer(), nullable=True),
        sa.Column("gas_avg_daily", sa.Numeric(precision=19, scale=2), nullable=True),
        sa.Column("water_avg_daily", sa.Numeric(precision=19, scale=2), nullable=True),
        sa.Column("boe_avg_daily", sa.Numeric(precision=19, scale=2), nullable=True),
        sa.Column(
            "comments",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default="[]",
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("api10", "prod_date"),
    )
    op.create_table(
        "runtime_stats",
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
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("hole_direction", sa.String(length=25), nullable=True),
        sa.Column("data_type", sa.String(length=25), nullable=True),
        sa.Column("operation", sa.String(length=50), nullable=True),
        sa.Column("name", sa.String(length=50), nullable=True),
        sa.Column("time", sa.Numeric(precision=19, scale=2), nullable=True),
        sa.Column("count", sa.Integer(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "shapes",
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
        sa.Column("api14", sa.String(length=14), nullable=False),
        sa.Column(
            "shl",
            geoalchemy2.types.Geometry(geometry_type="POINT", srid=4326),
            nullable=True,
        ),
        sa.Column(
            "kop",
            geoalchemy2.types.Geometry(geometry_type="POINT", srid=4326),
            nullable=True,
        ),
        sa.Column(
            "heel",
            geoalchemy2.types.Geometry(geometry_type="POINT", srid=4326),
            nullable=True,
        ),
        sa.Column(
            "mid",
            geoalchemy2.types.Geometry(geometry_type="POINT", srid=4326),
            nullable=True,
        ),
        sa.Column(
            "toe",
            geoalchemy2.types.Geometry(geometry_type="POINT", srid=4326),
            nullable=True,
        ),
        sa.Column(
            "bhl",
            geoalchemy2.types.Geometry(geometry_type="POINT", srid=4326),
            nullable=True,
        ),
        sa.Column(
            "wellbore",
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
            "lateral_only",
            geoalchemy2.types.Geometry(geometry_type="LINESTRING", srid=4326),
            nullable=True,
        ),
        sa.Column(
            "shl_webmercator",
            geoalchemy2.types.Geometry(geometry_type="POINT", srid=3857),
            nullable=True,
        ),
        sa.Column(
            "kop_webmercator",
            geoalchemy2.types.Geometry(geometry_type="POINT", srid=3857),
            nullable=True,
        ),
        sa.Column(
            "heel_webmercator",
            geoalchemy2.types.Geometry(geometry_type="POINT", srid=3857),
            nullable=True,
        ),
        sa.Column(
            "mid_webmercator",
            geoalchemy2.types.Geometry(geometry_type="POINT", srid=3857),
            nullable=True,
        ),
        sa.Column(
            "toe_webmercator",
            geoalchemy2.types.Geometry(geometry_type="POINT", srid=3857),
            nullable=True,
        ),
        sa.Column(
            "bhl_webmercator",
            geoalchemy2.types.Geometry(geometry_type="POINT", srid=3857),
            nullable=True,
        ),
        sa.Column(
            "wellbore_webmercator",
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
        sa.Column(
            "lateral_only_webmercator",
            geoalchemy2.types.Geometry(geometry_type="LINESTRING", srid=3857),
            nullable=True,
        ),
        sa.PrimaryKeyConstraint("api14"),
    )
    op.create_index(op.f("ix_shapes_api14"), "shapes", ["api14"], unique=False)
    op.create_table(
        "well_links",
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
        sa.Column("api14", sa.String(length=14), nullable=False),
        sa.Column("name", sa.String(length=50), nullable=False),
        sa.Column("value", sa.String(), nullable=True),
        sa.PrimaryKeyConstraint("api14", "name"),
    )
    op.create_index(op.f("ix_well_links_api14"), "well_links", ["api14"], unique=False)
    op.create_index(op.f("ix_well_links_name"), "well_links", ["name"], unique=False)
    op.create_table(
        "wells",
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
        sa.Column("api14", sa.String(length=14), nullable=False),
        sa.Column("api10", sa.String(length=10), nullable=True),
        sa.Column("hole_direction", sa.String(length=1), nullable=True),
        sa.Column("status", sa.String(length=50), nullable=True),
        sa.Column("is_producing", sa.Boolean(), nullable=True),
        sa.Column("operator", sa.String(), nullable=True),
        sa.Column("hist_operator", sa.String(), nullable=True),
        sa.Column("primary_product", sa.String(length=10), nullable=True),
        sa.Column("perfll", sa.Integer(), nullable=True),
        sa.Column("lateral_length", sa.Integer(), nullable=True),
        sa.Column("frac_bbl", sa.Integer(), nullable=True),
        sa.Column("frac_lb", sa.Integer(), nullable=True),
        sa.Column("frac_bbl_ft", sa.Integer(), nullable=True),
        sa.Column("frac_lb_ft", sa.Integer(), nullable=True),
        sa.Column("frac_gen_int", sa.Integer(), nullable=True),
        sa.Column("frac_gen_str", sa.String(), nullable=True),
        sa.Column("comp_date", sa.Date(), nullable=True),
        sa.Column("spud_date", sa.Date(), nullable=True),
        sa.Column("permit_date", sa.Date(), nullable=True),
        sa.Column("abandoned_date", sa.Date(), nullable=True),
        sa.Column("rig_release_date", sa.Date(), nullable=True),
        sa.Column("last_activity_date", sa.Date(), nullable=True),
        sa.Column("basin", sa.String(length=50), nullable=True),
        sa.Column("state", sa.String(length=50), nullable=True),
        sa.Column("county", sa.String(length=50), nullable=True),
        sa.Column("shllon", sa.Float(), nullable=True),
        sa.Column("shllat", sa.Float(), nullable=True),
        sa.Column("bhllon", sa.Float(), nullable=True),
        sa.Column("bhllat", sa.Float(), nullable=True),
        sa.PrimaryKeyConstraint("api14"),
    )
    op.create_index(op.f("ix_wells_api10"), "wells", ["api10"], unique=False)
    op.create_index(op.f("ix_wells_api14"), "wells", ["api14"], unique=False)
    op.create_index(op.f("ix_wells_basin"), "wells", ["basin"], unique=False)
    op.create_index(op.f("ix_wells_county"), "wells", ["county"], unique=False)
    op.create_index(
        op.f("ix_wells_hist_operator"), "wells", ["hist_operator"], unique=False
    )
    op.create_index(op.f("ix_wells_operator"), "wells", ["operator"], unique=False)
    op.create_index(
        "well_basin_holedir_isprod_idx",
        "wells",
        ["basin", "hole_direction", "is_producing"],
        unique=False,
    )
    op.create_index("well_basin_status_idx", "wells", ["basin", "status"], unique=False)
    op.create_table(
        "wellstats",
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
        sa.Column("api14", sa.String(length=14), nullable=False),
        sa.Column("wellbore_crow_length", sa.Integer(), nullable=True),
        sa.Column("wellbore_direction", sa.String(length=1), nullable=True),
        sa.Column("wellbore_bearing", sa.Float(), nullable=True),
        sa.Column("wellbore_dls_roc", sa.Float(), nullable=True),
        sa.Column("lateral_dls_roc", sa.Float(), nullable=True),
        sa.Column("wellbore_dls_mc", sa.Float(), nullable=True),
        sa.Column("lateral_dls_mc", sa.Float(), nullable=True),
        sa.Column("nearest_deo_prospect", sa.String(length=50), nullable=True),
        sa.Column("dist_to_deo_prospect_mi", sa.Float(), nullable=True),
        sa.Column("nearest_deo_api10", sa.String(length=50), nullable=True),
        sa.Column("dist_to_deo_well_mi", sa.Float(), nullable=True),
        sa.PrimaryKeyConstraint("api14"),
    )
    op.create_index(op.f("ix_wellstats_api14"), "wellstats", ["api14"], unique=False)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f("ix_wellstats_api14"), table_name="wellstats")
    op.drop_table("wellstats")
    op.drop_index("well_basin_status_idx", table_name="wells")
    op.drop_index("well_basin_holedir_isprod_idx", table_name="wells")
    op.drop_index(op.f("ix_wells_operator"), table_name="wells")
    op.drop_index(op.f("ix_wells_hist_operator"), table_name="wells")
    op.drop_index(op.f("ix_wells_county"), table_name="wells")
    op.drop_index(op.f("ix_wells_basin"), table_name="wells")
    op.drop_index(op.f("ix_wells_api14"), table_name="wells")
    op.drop_index(op.f("ix_wells_api10"), table_name="wells")
    op.drop_table("wells")
    op.drop_index(op.f("ix_well_links_name"), table_name="well_links")
    op.drop_index(op.f("ix_well_links_api14"), table_name="well_links")
    op.drop_table("well_links")
    op.drop_index(op.f("ix_shapes_api14"), table_name="shapes")
    op.drop_table("shapes")
    op.drop_table("runtime_stats")
    op.drop_table("production_monthly")
    op.drop_table("production_header")
    op.drop_table("prodstats")
    op.drop_index(op.f("ix_depths_name"), table_name="depths")
    op.drop_index(op.f("ix_depths_api14"), table_name="depths")
    op.drop_table("depths")
    op.drop_table("areas")
    # ### end Alembic commands ###