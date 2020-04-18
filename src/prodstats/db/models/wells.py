from db.models.bases import Base, db

__all__ = [
    "WellHeader",
    "WellStat",
    "WellDepth",
    "WellLink",
    "WellLocation",
    "IPTest",
    "FracParameters",
    "Survey",
    "SurveyPoint",
]


class WellBase(Base):
    api14 = db.Column(db.String(14), index=True, primary_key=True)


class WellHeader(WellBase):
    __tablename__ = "wells"

    api10 = db.Column(db.String(10), index=True)
    well_name = db.Column(db.String())
    hole_direction = db.Column(db.String(1))
    status = db.Column(db.String(50))
    is_producing = db.Column(db.Boolean(), index=True)
    operator = db.Column(db.String(), index=True)
    operator_alias = db.Column(db.String(), index=True)
    hist_operator = db.Column(db.String(), index=True)
    hist_operator_alias = db.Column(db.String(), index=True)
    tvd = db.Column(db.Integer())
    md = db.Column(db.Integer())
    perfll = db.Column(db.Integer())
    lateral_length = db.Column(db.Integer())
    ground_elev = db.Column(db.Integer())
    kb_elev = db.Column(db.Integer())
    comp_date = db.Column(db.Date())
    spud_date = db.Column(db.Date())
    permit_date = db.Column(db.Date())
    permit_number = db.Column(db.String())
    permit_status = db.Column(db.String())
    rig_release_date = db.Column(db.Date())  # rr_date
    last_activity_date = db.Column(db.Date())
    basin = db.Column(db.String(50), index=True)  # basin
    sub_basin = db.Column(db.String(50), index=True)  # basin
    state = db.Column(db.String(50))  # state_name
    state_code = db.Column(db.String(10))  # state_name
    county = db.Column(db.String(50), index=True)  # county_name
    county_code = db.Column(db.String(10))  # county_name
    provider_status = db.Column(db.String(50))
    provider = db.Column(db.String())
    provider_last_update_at = db.Column(db.DateTime(timezone=True))
    basin_holedir_isprod_idx = db.Index(
        "ix_well_basin_holedir_isprod", "basin", "hole_direction", "is_producing"
    )
    basin_status_idx = db.Index("ix_well_basin_status", "basin", "status")


class FracParameters(WellBase):
    __tablename__ = "frac_parameters"

    fluid_bbl = db.Column(db.Integer())
    proppant_lb = db.Column(db.Integer())
    fluid_bbl_ft = db.Column(db.Integer())
    proppant_lb_ft = db.Column(db.Integer())
    lateral_length = db.Column(db.Integer())
    lateral_length_type = db.Column(db.String(25))
    gen = db.Column(db.Integer())
    gen_name = db.Column(db.String(10))
    provider = db.Column(db.String())
    provider_last_update_at = db.Column(db.DateTime(timezone=True))


class WellStat(WellBase):
    __tablename__ = "wellstats"

    name = db.Column(db.String(50), primary_key=True)
    type = db.Column(db.String(25), nullable=False)  # numeric, string, date
    numeric_value = db.Column(db.Numeric(19, 2))
    string_value = db.Column(db.Numeric(19, 2))
    date_value = db.Column(db.Numeric(19, 2))
    comments = db.Column(db.JSONB(), nullable=False, server_default="{}")

    # wellbore_crow_length = db.Column(db.Integer())  # wellbore_linear_distance
    # wellbore_direction = db.Column(db.String(1))  # wellbore_direction
    # wellbore_bearing = db.Column(db.Float())  # wellbore_direction_degrees
    # wellbore_dls_roc = db.Column(db.Float())
    # lateral_dls_roc = db.Column(db.Float())
    # wellbore_dls_mc = db.Column(db.Float())
    # lateral_dls_mc = db.Column(db.Float())
    # nearest_prospect = db.Column(db.String(50))
    # dist_to_prospect_mi = db.Column(db.Float())
    # nearest_api10 = db.Column(db.String(50))
    # dist_to_deo_well_mi = db.Column(db.Float())


class WellDepth(WellBase):
    __tablename__ = "depths"

    name = db.Column(db.String(50), index=True, primary_key=True)
    value = db.Column(db.Integer())
    property_name = db.Column(db.String(50), index=True)
    aggregate_type = db.Column(db.String(25))
    grid_id = db.Column(db.Integer(), index=True)
    formation = db.Column(db.String(50), index=True)
    into_formation_feet = db.Column(db.Integer())
    into_formation_percent = db.Column(db.Float())
    above_next_formation_feet = db.Column(db.Integer())
    above_next_formation_percent = db.Column(db.Float())
    overlap_feet = db.Column(db.Integer())
    overlap_percent = db.Column(db.Float())
    in_target = db.Column(db.Boolean())
    assignment_method = db.Column(db.String())  # TODO: enum


class WellLink(WellBase):
    __tablename__ = "well_links"

    name = db.Column(db.String(50), index=True, primary_key=True)
    value = db.Column(db.String())


class WellLocation(WellBase):
    __tablename__ = "well_locations"

    name = db.Column(db.String(50), index=True, primary_key=True)
    block = db.Column(db.String(50))
    section = db.Column(db.String(50))
    abstract = db.Column(db.String(50))
    survey = db.Column(db.String(50))
    metes_bounds = db.Column(db.String(50))
    lon = db.Column(db.Float())
    lat = db.Column(db.Float())
    geom = db.Column(db.Geometry("POINT", srid=4326))


class Survey(WellBase):
    __tablename__ = "surveys"

    survey_type = db.Column(db.String(50))
    survey_method = db.Column(db.String(50))
    survey_date = db.Column(db.Date())
    survey_top = db.Column(db.Integer())
    survey_top_uom = db.Column(db.String(10))
    survey_base = db.Column(db.Integer())
    survey_base_uom = db.Column(db.String(10))
    wellbore = db.Column(db.Geometry("LINESTRING", srid=4326))
    lateral_only = db.Column(db.Geometry("LINESTRING", srid=4326))
    stick = db.Column(db.Geometry("LINESTRING", srid=4326))
    bent_stick = db.Column(db.Geometry("LINESTRING", srid=4326))


class SurveyPoint(WellBase):
    __tablename__ = "survey_points"

    md = db.Column(db.Integer(), primary_key=True)
    tvd = db.Column(db.Integer())
    dip = db.Column(db.Float())
    sequence = db.Column(db.Integer())
    theta = db.Column(db.Float())
    is_in_lateral = db.Column(db.Boolean(), nullable=False, default=False)
    is_heel_point = db.Column(db.Boolean(), nullable=False, default=False)
    is_mid_point = db.Column(db.Boolean(), nullable=False, default=False)
    is_toe_point = db.Column(db.Boolean(), nullable=False, default=False)
    is_soft_corner = db.Column(db.Boolean(), nullable=False, default=False)
    is_hard_corner = db.Column(db.Boolean(), nullable=False, default=False)
    is_kop = db.Column(db.Boolean(), nullable=False, default=False)
    geom = db.Column(db.Geometry("POINT", srid=4326))
    ix_lateral_partial = db.Index(
        "ix_lateral_partial",
        "api14",
        "is_in_lateral",
        postgresql_where=(is_in_lateral),
    )
    ix_heel_partial = db.Index(
        "ix_heel_partial", "api14", "is_heel_point", postgresql_where=(is_heel_point),
    )
    ix_mid_partial = db.Index(
        "ix_mid_partial", "api14", "is_mid_point", postgresql_where=(is_mid_point),
    )
    ix_toe_partial = db.Index(
        "ix_toe_partial", "api14", "is_toe_point", postgresql_where=(is_toe_point),
    )


class IPTest(WellBase):
    __tablename__ = "ip_tests"

    test_number = db.Column(db.Integer(), primary_key=True)
    test_date = db.Column(db.Date())
    type_code = db.Column(db.String(10))
    test_method = db.Column(db.String())
    completion = db.Column(db.Integer())
    oil = db.Column(db.Integer())
    oil_per10k = db.Column(db.Integer())
    oil_uom = db.Column(db.String(10))
    gas = db.Column(db.Integer())
    gas_per10k = db.Column(db.Integer())
    gas_uom = db.Column(db.String(10))
    water = db.Column(db.Integer())
    water_per10k = db.Column(db.Integer())
    water_uom = db.Column(db.String(10))
    choke = db.Column(db.String(25))
    depth_top = db.Column(db.Integer())
    depth_top_uom = db.Column(db.String(10))
    depth_base = db.Column(db.Integer())
    depth_base_uom = db.Column(db.String(10))
    sulfur = db.Column(db.Boolean())
    oil_gravity = db.Column(db.Float())
    oil_gravity_uom = db.Column(db.String(10))
    gor = db.Column(db.Integer())
    gor_uom = db.Column(db.String(10))
    perf_upper = db.Column(db.Integer())
    perf_upper_uom = db.Column(db.String(10))
    perf_lower = db.Column(db.Integer())
    perf_lower_uom = db.Column(db.String(10))
    perfll = db.Column(db.Integer())
    perfll_uom = db.Column(db.String(10))
