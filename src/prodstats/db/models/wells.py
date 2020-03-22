from db.models.bases import Base, db

__all__ = ["WellHeader", "WellStat", "WellDepth", "WellLink", "WellShape"]


class WellBase(Base):
    api14 = db.Column(db.String(14), index=True, primary_key=True)


class WellHeader(WellBase):
    __tablename__ = "wells"

    api10 = db.Column(db.String(10), index=True)
    hole_direction = db.Column(db.String(1))
    status = db.Column(db.String(50))
    is_producing = db.Column(db.Boolean())
    operator = db.Column(db.String(), index=True)
    hist_operator = db.Column(db.String(), index=True)
    perfll = db.Column(db.Integer())
    lateral_length = db.Column(db.Integer())
    comp_date = db.Column(db.Date())
    spud_date = db.Column(db.Date())
    permit_date = db.Column(db.Date())
    rig_release_date = db.Column(db.Date())  # rr_date
    last_activity_date = db.Column(db.Date())
    basin = db.Column(db.String(50), index=True)  # basin
    state = db.Column(db.String(50))  # state_name
    county = db.Column(db.String(50), index=True)  # county_name
    basin_holedir_isprod_idx = db.Index(
        "well_basin_holedir_isprod_idx", "basin", "hole_direction", "is_producing"
    )
    basin_status_idx = db.Index("well_basin_status_idx", "basin", "status")


class FracParameters(WellBase):
    __tablename__ = "frac_parameters"

    fluid_bbl = db.Column(db.Integer())
    proppant_lb = db.Column(db.Integer())
    fluid_bbl_ft = db.Column(db.Integer())
    proppant_lb_ft = db.Column(db.Integer())
    gen_int = db.Column(db.Integer())
    gen_str = db.Column(db.String(10))


class WellStat(WellBase):
    __tablename__ = "wellstats"

    wellbore_crow_length = db.Column(db.Integer())  # wellbore_linear_distance
    wellbore_direction = db.Column(db.String(1))  # wellbore_direction
    wellbore_bearing = db.Column(db.Float())  # wellbore_direction_degrees
    wellbore_dls_roc = db.Column(db.Float())
    lateral_dls_roc = db.Column(db.Float())
    wellbore_dls_mc = db.Column(db.Float())
    lateral_dls_mc = db.Column(db.Float())
    nearest_deo_prospect = db.Column(db.String(50))
    dist_to_deo_prospect_mi = db.Column(db.Float())
    nearest_deo_api10 = db.Column(db.String(50))
    dist_to_deo_well_mi = db.Column(db.Float())


class WellDepth(WellBase):
    __tablename__ = "depths"

    name = db.Column(db.String(50), index=True, primary_key=True)
    value = db.Column(db.Integer())
    grid_id = db.Column(db.Integer())
    formation = db.Column(db.String(50))
    into_formation_feet = db.Column(db.Integer())
    into_formation_percent = db.Column(db.Float())
    above_next_formation_feet = db.Column(db.Integer())
    above_next_formation_percent = db.Column(db.Float())
    overlap_feet = db.Column(db.Integer())
    overlap_percent = db.Column(db.Float())
    in_target = db.Column(db.Boolean())
    # tvd = db.Column(db.Integer())
    # td = db.Column(db.Integer())
    # tvd_min = db.Column(db.Integer())
    # tvd_max = db.Column(db.Integer())
    # tvd_avg = db.Column(db.Integer())
    # tvd_heel = db.Column(db.Integer())
    # tvd_mid = db.Column(db.Integer())  # tvd_midpoint
    # tvd_toe = db.Column(db.Integer())
    # md_min = db.Column(db.Integer())
    # md_max = db.Column(db.Integer())
    # md_avg = db.Column(db.Integer())
    # perf_upper_min = db.Column(db.Integer())  # perf_upper_min
    # perf_lower_max = db.Column(db.Integer())  # perf_lower_max
    # top_min = db.Column(db.Integer())  # well_depthtop_min
    # base_max = db.Column(db.Integer())  # well_depthbase_max


class WellLink(WellBase):
    __tablename__ = "well_links"

    name = db.Column(db.String(50), index=True, primary_key=True)
    value = db.Column(db.String())


class WellShape(WellBase):
    __tablename__ = "shapes"

    shllon = db.Column(db.Float())  # long_shl
    shllat = db.Column(db.Float())  # lat_shl
    bhllon = db.Column(db.Float())  # long_bhl
    bhllat = db.Column(db.Float())  # lat_bhl
    shl = db.Column(db.Geometry("POINT", srid=4326))
    kop = db.Column(db.Geometry("POINT", srid=4326))
    heel = db.Column(db.Geometry("POINT", srid=4326))
    mid = db.Column(db.Geometry("POINT", srid=4326))
    toe = db.Column(db.Geometry("POINT", srid=4326))
    bhl = db.Column(db.Geometry("POINT", srid=4326))
    wellbore = db.Column(db.Geometry("LINESTRING", srid=4326))
    stick = db.Column(db.Geometry("LINESTRING", srid=4326))
    bent_stick = db.Column(db.Geometry("LINESTRING", srid=4326))
    lateral_only = db.Column(db.Geometry("LINESTRING", srid=4326))

    shl_webmercator = db.Column(db.Geometry("POINT", srid=3857))
    kop_webmercator = db.Column(db.Geometry("POINT", srid=3857))
    heel_webmercator = db.Column(db.Geometry("POINT", srid=3857))
    mid_webmercator = db.Column(db.Geometry("POINT", srid=3857))
    toe_webmercator = db.Column(db.Geometry("POINT", srid=3857))
    bhl_webmercator = db.Column(db.Geometry("POINT", srid=3857))
    wellbore_webmercator = db.Column(db.Geometry("LINESTRING", srid=3857))
    stick_webmercator = db.Column(db.Geometry("LINESTRING", srid=3857))
    bent_stick_webmercator = db.Column(db.Geometry("LINESTRING", srid=3857))
    lateral_only_webmercator = db.Column(db.Geometry("LINESTRING", srid=3857))


class IPTest(WellBase):
    test_number = db.Column(db.Integer(), primary_key=True)
    test_date = db.Column(db.Date())
    type_code = db.Column(db.String(10))
    test_method = db.Column(db.String())
    completion = db.Column(db.Integer())
    oil = db.Column(db.Integer())
    oil_uom = db.Column(db.String(10))
    gas = db.Column(db.Integer())
    gas_uom = db.Column(db.String(10))
    water = db.Column(db.Integer())
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
