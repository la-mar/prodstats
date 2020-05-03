"""creating base tables

Revision ID: 0445df8a3cbd
Revises: 244869cf6945
Create Date: 2020-04-26 19:46:17.382457+00:00

"""
import geoalchemy2
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "0445df8a3cbd"
down_revision = "244869cf6945"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        "areas",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("area", sa.String(length=25), nullable=False),
        sa.Column("h_last_run_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("v_last_run_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "providers",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default="[]",
            nullable=False,
        ),
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
        sa.PrimaryKeyConstraint("id", name=op.f("pk_areas")),
        sa.UniqueConstraint("area"),
        sa.UniqueConstraint("area", name=op.f("uq_areas_area")),
    )
    op.create_table(
        "depths",
        sa.Column("api14", sa.String(length=14), nullable=False),
        sa.Column("name", sa.String(length=50), nullable=False),
        sa.Column("value", sa.Integer(), nullable=True),
        sa.Column("property_name", sa.String(length=50), nullable=True),
        sa.Column("aggregate_type", sa.String(length=25), nullable=True),
        sa.Column("grid_id", sa.Integer(), nullable=True),
        sa.Column("formation", sa.String(length=50), nullable=True),
        sa.Column("into_formation_feet", sa.Integer(), nullable=True),
        sa.Column("into_formation_percent", sa.Float(), nullable=True),
        sa.Column("above_next_formation_feet", sa.Integer(), nullable=True),
        sa.Column("above_next_formation_percent", sa.Float(), nullable=True),
        sa.Column("overlap_feet", sa.Integer(), nullable=True),
        sa.Column("overlap_percent", sa.Float(), nullable=True),
        sa.Column("in_target", sa.Boolean(), nullable=True),
        sa.Column("assignment_method", sa.String(), nullable=True),
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
        sa.PrimaryKeyConstraint("api14", "name", name=op.f("pk_depths")),
    )
    op.create_index(op.f("ix_depths_api14"), "depths", ["api14"], unique=False)
    op.create_index(op.f("ix_depths_formation"), "depths", ["formation"], unique=False)
    op.create_index(op.f("ix_depths_grid_id"), "depths", ["grid_id"], unique=False)
    op.create_index(op.f("ix_depths_name"), "depths", ["name"], unique=False)
    op.create_index(
        op.f("ix_depths_property_name"), "depths", ["property_name"], unique=False
    )
    op.create_table(
        "frac_parameters",
        sa.Column("api14", sa.String(length=14), nullable=False),
        sa.Column("fluid_bbl", sa.Integer(), nullable=True),
        sa.Column("proppant_lb", sa.Integer(), nullable=True),
        sa.Column("fluid_bbl_ft", sa.Integer(), nullable=True),
        sa.Column("proppant_lb_ft", sa.Integer(), nullable=True),
        sa.Column("lateral_length", sa.Integer(), nullable=True),
        sa.Column("lateral_length_type", sa.String(length=25), nullable=True),
        sa.Column("gen", sa.Integer(), nullable=True),
        sa.Column("gen_name", sa.String(length=10), nullable=True),
        sa.Column("provider", sa.String(), nullable=True),
        sa.Column("provider_last_update_at", sa.DateTime(timezone=True), nullable=True),
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
        sa.PrimaryKeyConstraint("api14", name=op.f("pk_frac_parameters")),
    )
    op.create_index(
        op.f("ix_frac_parameters_api14"), "frac_parameters", ["api14"], unique=False
    )
    op.create_table(
        "ip_tests",
        sa.Column("api14", sa.String(length=14), nullable=False),
        sa.Column("test_number", sa.Integer(), nullable=False),
        sa.Column("test_date", sa.Date(), nullable=True),
        sa.Column("type_code", sa.String(length=10), nullable=True),
        sa.Column("test_method", sa.String(), nullable=True),
        sa.Column("completion", sa.Integer(), nullable=True),
        sa.Column("oil", sa.Integer(), nullable=True),
        sa.Column("oil_per10k", sa.Integer(), nullable=True),
        sa.Column("oil_uom", sa.String(length=10), nullable=True),
        sa.Column("gas", sa.Integer(), nullable=True),
        sa.Column("gas_per10k", sa.Integer(), nullable=True),
        sa.Column("gas_uom", sa.String(length=10), nullable=True),
        sa.Column("water", sa.Integer(), nullable=True),
        sa.Column("water_per10k", sa.Integer(), nullable=True),
        sa.Column("water_uom", sa.String(length=10), nullable=True),
        sa.Column("choke", sa.String(length=25), nullable=True),
        sa.Column("depth_top", sa.Integer(), nullable=True),
        sa.Column("depth_top_uom", sa.String(length=10), nullable=True),
        sa.Column("depth_base", sa.Integer(), nullable=True),
        sa.Column("depth_base_uom", sa.String(length=10), nullable=True),
        sa.Column("sulfur", sa.Boolean(), nullable=True),
        sa.Column("oil_gravity", sa.Float(), nullable=True),
        sa.Column("oil_gravity_uom", sa.String(length=10), nullable=True),
        sa.Column("gor", sa.Integer(), nullable=True),
        sa.Column("gor_uom", sa.String(length=10), nullable=True),
        sa.Column("perf_upper", sa.Integer(), nullable=True),
        sa.Column("perf_upper_uom", sa.String(length=10), nullable=True),
        sa.Column("perf_lower", sa.Integer(), nullable=True),
        sa.Column("perf_lower_uom", sa.String(length=10), nullable=True),
        sa.Column("perfll", sa.Integer(), nullable=True),
        sa.Column("perfll_uom", sa.String(length=10), nullable=True),
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
        sa.PrimaryKeyConstraint("api14", "test_number", name=op.f("pk_ip_tests")),
    )
    op.create_index(op.f("ix_ip_tests_api14"), "ip_tests", ["api14"], unique=False)
    op.create_table(
        "prodstats",
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
        sa.PrimaryKeyConstraint("api10", "name", name=op.f("pk_prodstats")),
    )
    op.create_index(
        "ix_prodstat_api10_prop_agg",
        "prodstats",
        ["api10", "property_name", "aggregate_type"],
        unique=False,
    )
    op.create_table(
        "production_header",
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
        sa.Column("oil_pdp_last3mo_per30kbbl", sa.Integer(), nullable=True),
        sa.Column("boe_pdp_last3mo_per30kbbl", sa.Integer(), nullable=True),
        sa.Column("products", sa.String(), nullable=True),
        sa.Column("provider", sa.String(), nullable=True),
        sa.Column("provider_last_update_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("related_well_count", sa.Integer(), nullable=True),
        sa.Column(
            "related_wells",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default="[]",
            nullable=False,
        ),
        sa.Column(
            "comments",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default="{}",
            nullable=False,
        ),
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
        sa.PrimaryKeyConstraint("api10", name=op.f("pk_production_header")),
    )
    op.create_index(
        op.f("ix_production_header_primary_api14"),
        "production_header",
        ["primary_api14"],
        unique=False,
    )
    op.create_table(
        "production_monthly",
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
        sa.Column("oil_per1k", sa.Integer(), nullable=True),
        sa.Column("gas_per1k", sa.Integer(), nullable=True),
        sa.Column("water_per1k", sa.Integer(), nullable=True),
        sa.Column("boe_per1k", sa.Integer(), nullable=True),
        sa.Column("gas_per3k", sa.Integer(), nullable=True),
        sa.Column("oil_per3k", sa.Integer(), nullable=True),
        sa.Column("water_per3k", sa.Integer(), nullable=True),
        sa.Column("boe_per3k", sa.Integer(), nullable=True),
        sa.Column("gas_per5k", sa.Integer(), nullable=True),
        sa.Column("oil_per5k", sa.Integer(), nullable=True),
        sa.Column("water_per5k", sa.Integer(), nullable=True),
        sa.Column("boe_per5k", sa.Integer(), nullable=True),
        sa.Column("gas_per7500", sa.Integer(), nullable=True),
        sa.Column("oil_per7500", sa.Integer(), nullable=True),
        sa.Column("water_per7500", sa.Integer(), nullable=True),
        sa.Column("boe_per7500", sa.Integer(), nullable=True),
        sa.Column("gas_per10k", sa.Integer(), nullable=True),
        sa.Column("oil_per10k", sa.Integer(), nullable=True),
        sa.Column("water_per10k", sa.Integer(), nullable=True),
        sa.Column("boe_per10k", sa.Integer(), nullable=True),
        sa.Column("oil_avg_daily", sa.Integer(), nullable=True),
        sa.Column("gas_avg_daily", sa.Numeric(precision=19, scale=2), nullable=True),
        sa.Column("water_avg_daily", sa.Numeric(precision=19, scale=2), nullable=True),
        sa.Column("boe_avg_daily", sa.Numeric(precision=19, scale=2), nullable=True),
        sa.Column(
            "comments",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default="{}",
            nullable=False,
        ),
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
        sa.PrimaryKeyConstraint(
            "api10", "prod_date", name=op.f("pk_production_monthly")
        ),
    )
    op.create_table(
        "runtime_stats",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("hole_direction", sa.String(length=25), nullable=True),
        sa.Column("data_type", sa.String(length=25), nullable=True),
        sa.Column("operation", sa.String(length=50), nullable=True),
        sa.Column("name", sa.String(length=50), nullable=True),
        sa.Column("time", sa.Numeric(precision=19, scale=2), nullable=True),
        sa.Column("count", sa.Integer(), nullable=True),
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
        sa.PrimaryKeyConstraint("id", name=op.f("pk_runtime_stats")),
    )
    op.create_table(
        "survey_points",
        sa.Column("api14", sa.String(length=14), nullable=False),
        sa.Column("md", sa.Integer(), nullable=False),
        sa.Column("tvd", sa.Integer(), nullable=True),
        sa.Column("dip", sa.Float(), nullable=True),
        sa.Column("sequence", sa.Integer(), nullable=True),
        sa.Column("theta", sa.Float(), nullable=True),
        sa.Column("is_in_lateral", sa.Boolean(), nullable=False),
        sa.Column("is_heel_point", sa.Boolean(), nullable=False),
        sa.Column("is_mid_point", sa.Boolean(), nullable=False),
        sa.Column("is_toe_point", sa.Boolean(), nullable=False),
        sa.Column("is_soft_corner", sa.Boolean(), nullable=False),
        sa.Column("is_hard_corner", sa.Boolean(), nullable=False),
        sa.Column("is_kop", sa.Boolean(), nullable=False),
        sa.Column(
            "geom",
            geoalchemy2.types.Geometry(
                geometry_type="POINT", srid=4326, spatial_index=False
            ),
            nullable=True,
        ),
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
        sa.PrimaryKeyConstraint("api14", "md", name=op.f("pk_survey_points")),
    )
    op.create_index(
        "ix_heel_partial",
        "survey_points",
        ["api14", "is_heel_point"],
        unique=False,
        postgresql_where=sa.text("is_heel_point"),
    )
    op.create_index(
        "ix_lateral_partial",
        "survey_points",
        ["api14", "is_in_lateral"],
        unique=False,
        postgresql_where=sa.text("is_in_lateral"),
    )
    op.create_index(
        "ix_mid_partial",
        "survey_points",
        ["api14", "is_mid_point"],
        unique=False,
        postgresql_where=sa.text("is_mid_point"),
    )
    op.create_index(
        "ix_survey_point_geom",
        "survey_points",
        ["geom"],
        unique=False,
        postgresql_using="gist",
    )
    op.create_index(
        op.f("ix_survey_points_api14"), "survey_points", ["api14"], unique=False
    )
    op.create_index(
        "ix_toe_partial",
        "survey_points",
        ["api14", "is_toe_point"],
        unique=False,
        postgresql_where=sa.text("is_toe_point"),
    )
    op.create_table(
        "surveys",
        sa.Column("api14", sa.String(length=14), nullable=False),
        sa.Column("survey_type", sa.String(length=50), nullable=True),
        sa.Column("survey_method", sa.String(length=50), nullable=True),
        sa.Column("survey_date", sa.Date(), nullable=True),
        sa.Column("survey_top", sa.Integer(), nullable=True),
        sa.Column("survey_top_uom", sa.String(length=10), nullable=True),
        sa.Column("survey_base", sa.Integer(), nullable=True),
        sa.Column("survey_base_uom", sa.String(length=10), nullable=True),
        sa.Column(
            "wellbore",
            geoalchemy2.types.Geometry(
                geometry_type="LINESTRING", srid=4326, spatial_index=False
            ),
            nullable=True,
        ),
        sa.Column(
            "lateral_only",
            geoalchemy2.types.Geometry(
                geometry_type="LINESTRING", srid=4326, spatial_index=False
            ),
            nullable=True,
        ),
        sa.Column(
            "stick",
            geoalchemy2.types.Geometry(
                geometry_type="LINESTRING", srid=4326, spatial_index=False
            ),
            nullable=True,
        ),
        sa.Column(
            "bent_stick",
            geoalchemy2.types.Geometry(
                geometry_type="LINESTRING", srid=4326, spatial_index=False
            ),
            nullable=True,
        ),
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
        sa.PrimaryKeyConstraint("api14", name=op.f("pk_surveys")),
    )
    op.create_index(
        "ix_survey_bent_stick",
        "surveys",
        ["bent_stick"],
        unique=False,
        postgresql_using="gist",
    )
    op.create_index(
        "ix_survey_lateral_only",
        "surveys",
        ["lateral_only"],
        unique=False,
        postgresql_using="gist",
    )
    op.create_index(
        "ix_survey_stick", "surveys", ["stick"], unique=False, postgresql_using="gist"
    )
    op.create_index(
        "ix_survey_wellbore",
        "surveys",
        ["wellbore"],
        unique=False,
        postgresql_using="gist",
    )
    op.create_index(op.f("ix_surveys_api14"), "surveys", ["api14"], unique=False)
    op.create_table(
        "well_links",
        sa.Column("api14", sa.String(length=14), nullable=False),
        sa.Column("name", sa.String(length=50), nullable=False),
        sa.Column("value", sa.String(), nullable=True),
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
        sa.PrimaryKeyConstraint("api14", "name", name=op.f("pk_well_links")),
    )
    op.create_index(op.f("ix_well_links_api14"), "well_links", ["api14"], unique=False)
    op.create_index(op.f("ix_well_links_name"), "well_links", ["name"], unique=False)
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
            "geom",
            geoalchemy2.types.Geometry(
                geometry_type="POINT", srid=4326, spatial_index=False
            ),
            nullable=True,
        ),
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
        sa.PrimaryKeyConstraint("api14", "name", name=op.f("pk_well_locations")),
    )
    op.create_index(
        "ix_well_location_geom",
        "well_locations",
        ["geom"],
        unique=False,
        postgresql_using="gist",
    )
    op.create_index(
        op.f("ix_well_locations_api14"), "well_locations", ["api14"], unique=False
    )
    op.create_index(
        op.f("ix_well_locations_name"), "well_locations", ["name"], unique=False
    )
    op.create_table(
        "wells",
        sa.Column("api14", sa.String(length=14), nullable=False),
        sa.Column("api10", sa.String(length=10), nullable=True),
        sa.Column("well_name", sa.String(), nullable=True),
        sa.Column("hole_direction", sa.String(length=1), nullable=True),
        sa.Column("status", sa.String(length=50), nullable=True),
        sa.Column("is_producing", sa.Boolean(), nullable=True),
        sa.Column("operator", sa.String(), nullable=True),
        sa.Column("operator_alias", sa.String(), nullable=True),
        sa.Column("hist_operator", sa.String(), nullable=True),
        sa.Column("hist_operator_alias", sa.String(), nullable=True),
        sa.Column("tvd", sa.Integer(), nullable=True),
        sa.Column("md", sa.Integer(), nullable=True),
        sa.Column("perfll", sa.Integer(), nullable=True),
        sa.Column("lateral_length", sa.Integer(), nullable=True),
        sa.Column("ground_elev", sa.Integer(), nullable=True),
        sa.Column("kb_elev", sa.Integer(), nullable=True),
        sa.Column("comp_date", sa.Date(), nullable=True),
        sa.Column("spud_date", sa.Date(), nullable=True),
        sa.Column("permit_date", sa.Date(), nullable=True),
        sa.Column("permit_number", sa.String(), nullable=True),
        sa.Column("permit_status", sa.String(), nullable=True),
        sa.Column("rig_release_date", sa.Date(), nullable=True),
        sa.Column("last_activity_date", sa.Date(), nullable=True),
        sa.Column("basin", sa.String(length=50), nullable=True),
        sa.Column("sub_basin", sa.String(length=50), nullable=True),
        sa.Column("state", sa.String(length=50), nullable=True),
        sa.Column("state_code", sa.String(length=10), nullable=True),
        sa.Column("county", sa.String(length=50), nullable=True),
        sa.Column("county_code", sa.String(length=10), nullable=True),
        sa.Column("provider_status", sa.String(length=50), nullable=True),
        sa.Column("provider", sa.String(), nullable=True),
        sa.Column("provider_last_update_at", sa.DateTime(timezone=True), nullable=True),
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
        sa.PrimaryKeyConstraint("api14", name=op.f("pk_wells")),
    )
    op.create_index(
        "ix_well_basin_holedir_isprod",
        "wells",
        ["basin", "hole_direction", "is_producing"],
        unique=False,
    )
    op.create_index("ix_well_basin_status", "wells", ["basin", "status"], unique=False)
    op.create_index(op.f("ix_wells_api10"), "wells", ["api10"], unique=False)
    op.create_index(op.f("ix_wells_api14"), "wells", ["api14"], unique=False)
    op.create_index(op.f("ix_wells_basin"), "wells", ["basin"], unique=False)
    op.create_index(op.f("ix_wells_county"), "wells", ["county"], unique=False)
    op.create_index(
        op.f("ix_wells_hist_operator"), "wells", ["hist_operator"], unique=False
    )
    op.create_index(
        op.f("ix_wells_hist_operator_alias"),
        "wells",
        ["hist_operator_alias"],
        unique=False,
    )
    op.create_index(
        op.f("ix_wells_is_producing"), "wells", ["is_producing"], unique=False
    )
    op.create_index(op.f("ix_wells_operator"), "wells", ["operator"], unique=False)
    op.create_index(
        op.f("ix_wells_operator_alias"), "wells", ["operator_alias"], unique=False
    )
    op.create_index(op.f("ix_wells_sub_basin"), "wells", ["sub_basin"], unique=False)
    op.create_table(
        "wellstats",
        sa.Column("api14", sa.String(length=14), nullable=False),
        sa.Column("name", sa.String(length=50), nullable=False),
        sa.Column("type", sa.String(length=25), nullable=False),
        sa.Column("numeric_value", sa.Numeric(precision=19, scale=2), nullable=True),
        sa.Column("string_value", sa.Numeric(precision=19, scale=2), nullable=True),
        sa.Column("date_value", sa.Numeric(precision=19, scale=2), nullable=True),
        sa.Column(
            "comments",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default="{}",
            nullable=False,
        ),
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
        sa.PrimaryKeyConstraint("api14", "name", name=op.f("pk_wellstats")),
    )
    op.create_index(op.f("ix_wellstats_api14"), "wellstats", ["api14"], unique=False)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f("ix_wellstats_api14"), table_name="wellstats")
    op.drop_table("wellstats")
    op.drop_index(op.f("ix_wells_sub_basin"), table_name="wells")
    op.drop_index(op.f("ix_wells_operator_alias"), table_name="wells")
    op.drop_index(op.f("ix_wells_operator"), table_name="wells")
    op.drop_index(op.f("ix_wells_is_producing"), table_name="wells")
    op.drop_index(op.f("ix_wells_hist_operator_alias"), table_name="wells")
    op.drop_index(op.f("ix_wells_hist_operator"), table_name="wells")
    op.drop_index(op.f("ix_wells_county"), table_name="wells")
    op.drop_index(op.f("ix_wells_basin"), table_name="wells")
    op.drop_index(op.f("ix_wells_api14"), table_name="wells")
    op.drop_index(op.f("ix_wells_api10"), table_name="wells")
    op.drop_index("ix_well_basin_status", table_name="wells")
    op.drop_index("ix_well_basin_holedir_isprod", table_name="wells")
    op.drop_table("wells")
    op.drop_index(op.f("ix_well_locations_name"), table_name="well_locations")
    op.drop_index(op.f("ix_well_locations_api14"), table_name="well_locations")
    op.drop_index("ix_well_location_geom", table_name="well_locations")
    op.drop_table("well_locations")
    op.drop_index(op.f("ix_well_links_name"), table_name="well_links")
    op.drop_index(op.f("ix_well_links_api14"), table_name="well_links")
    op.drop_table("well_links")
    op.drop_index(op.f("ix_surveys_api14"), table_name="surveys")
    op.drop_index("ix_survey_wellbore", table_name="surveys")
    op.drop_index("ix_survey_stick", table_name="surveys")
    op.drop_index("ix_survey_lateral_only", table_name="surveys")
    op.drop_index("ix_survey_bent_stick", table_name="surveys")
    op.drop_table("surveys")
    op.drop_index("ix_toe_partial", table_name="survey_points")
    op.drop_index(op.f("ix_survey_points_api14"), table_name="survey_points")
    op.drop_index("ix_survey_point_geom", table_name="survey_points")
    op.drop_index("ix_mid_partial", table_name="survey_points")
    op.drop_index("ix_lateral_partial", table_name="survey_points")
    op.drop_index("ix_heel_partial", table_name="survey_points")
    op.drop_table("survey_points")
    op.drop_table("runtime_stats")
    op.drop_table("production_monthly")
    op.drop_index(
        op.f("ix_production_header_primary_api14"), table_name="production_header"
    )
    op.drop_table("production_header")
    op.drop_index("ix_prodstat_api10_prop_agg", table_name="prodstats")
    op.drop_table("prodstats")
    op.drop_index(op.f("ix_ip_tests_api14"), table_name="ip_tests")
    op.drop_table("ip_tests")
    op.drop_index(op.f("ix_frac_parameters_api14"), table_name="frac_parameters")
    op.drop_table("frac_parameters")
    op.drop_index(op.f("ix_depths_property_name"), table_name="depths")
    op.drop_index(op.f("ix_depths_name"), table_name="depths")
    op.drop_index(op.f("ix_depths_grid_id"), table_name="depths")
    op.drop_index(op.f("ix_depths_formation"), table_name="depths")
    op.drop_index(op.f("ix_depths_api14"), table_name="depths")
    op.drop_table("depths")
    op.drop_table("areas")
    # ### end Alembic commands ###