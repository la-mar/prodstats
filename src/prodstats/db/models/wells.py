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
    # operator_alias = db.Column(db.String(50), index=True)
    hist_operator = db.Column(db.String(), index=True)
    # hist_operator_alias = db.Column(db.String(50), index=True)
    primary_product = db.Column(db.String(10))
    perfll = db.Column(db.Integer())
    lateral_length = db.Column(db.Integer())
    frac_bbl = db.Column(db.Integer())
    frac_lb = db.Column(db.Integer())
    frac_bbl_ft = db.Column(db.Integer())
    frac_lb_ft = db.Column(db.Integer())
    frac_gen_int = db.Column(db.Integer())
    frac_gen_str = db.Column(db.String())
    comp_date = db.Column(db.Date())
    spud_date = db.Column(db.Date())
    permit_date = db.Column(db.Date())
    abandoned_date = db.Column(db.Date())  # abnd_date
    rig_release_date = db.Column(db.Date())  # rr_date
    last_activity_date = db.Column(db.Date())
    basin = db.Column(db.String(50), index=True)  # basin
    state = db.Column(db.String(50))  # state_name
    county = db.Column(db.String(50), index=True)  # county_name
    shllon = db.Column(db.Float())  # long_shl
    shllat = db.Column(db.Float())  # lat_shl
    bhllon = db.Column(db.Float())  # long_bhl
    bhllat = db.Column(db.Float())  # lat_bhl
    basin_holedir_isprod_idx = db.Index(
        "well_basin_holedir_isprod_idx", "basin", "hole_direction", "is_producing"
    )
    basin_status_idx = db.Index("well_basin_status_idx", "basin", "status")


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
