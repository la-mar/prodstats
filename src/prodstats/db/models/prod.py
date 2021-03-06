from db.models.bases import Base, db

__all__ = ["ProdMonthly", "ProdStat", "ProdHeader"]


class ProdHeader(Base):
    __tablename__ = "production_header"

    api10 = db.Column(db.String(10), primary_key=True)
    primary_api14 = db.Column(db.String(14), index=True)
    entity12 = db.Column(db.String(12), nullable=False)
    status = db.Column(db.String())
    first_prod_date = db.Column(db.Date())
    last_prod_date = db.Column(db.Date())
    prod_months = db.Column(db.Integer())
    prod_days = db.Column(db.Integer())
    peak_norm_months = db.Column(db.Integer())
    peak_norm_days = db.Column(db.Integer())
    peak30_oil = db.Column(db.Integer())
    peak30_gas = db.Column(db.Integer())
    peak30_date = db.Column(db.Date())
    peak30_month = db.Column(db.Integer())
    perfll = db.Column(db.Integer())
    perf_upper = db.Column(db.Integer())
    perf_lower = db.Column(db.Integer())
    oil_pdp_last3mo_per30kbbl = db.Column(db.Integer())
    boe_pdp_last3mo_per30kbbl = db.Column(db.Integer())
    products = db.Column(db.String())
    provider = db.Column(db.String())
    provider_last_update_at = db.Column(db.DateTime(timezone=True))
    related_well_count = db.Column(db.Integer())
    related_wells = db.Column(db.JSONB(), nullable=False, server_default="[]")
    comments = db.Column(db.JSONB(), nullable=False, server_default="{}")


class ProdMonthly(Base):
    __tablename__ = "production_monthly"

    api10 = db.Column(db.String(10), primary_key=True)
    prod_date = db.Column(db.Date(), primary_key=True)
    prod_month = db.Column(db.Integer())
    days_in_month = db.Column(db.Integer())
    prod_days = db.Column(db.Integer())
    peak_norm_month = db.Column(db.Integer())
    peak_norm_days = db.Column(db.Integer())  # currently prod_day
    oil = db.Column(db.Integer())
    gas = db.Column(db.Integer())
    water = db.Column(db.Integer())
    boe = db.Column(db.Integer())
    water_cut = db.Column(db.Numeric(19, 2))
    oil_percent = db.Column(db.Numeric(19, 2))
    gor = db.Column(db.Integer())
    oil_per1k = db.Column(db.Integer())
    gas_per1k = db.Column(db.Integer())
    water_per1k = db.Column(db.Integer())
    boe_per1k = db.Column(db.Integer())
    gas_per3k = db.Column(db.Integer())
    oil_per3k = db.Column(db.Integer())
    water_per3k = db.Column(db.Integer())
    boe_per3k = db.Column(db.Integer())
    gas_per5k = db.Column(db.Integer())
    oil_per5k = db.Column(db.Integer())
    water_per5k = db.Column(db.Integer())
    boe_per5k = db.Column(db.Integer())
    gas_per7500 = db.Column(db.Integer())
    oil_per7500 = db.Column(db.Integer())
    water_per7500 = db.Column(db.Integer())
    boe_per7500 = db.Column(db.Integer())
    gas_per10k = db.Column(db.Integer())
    oil_per10k = db.Column(db.Integer())
    water_per10k = db.Column(db.Integer())
    boe_per10k = db.Column(db.Integer())
    oil_avg_daily = db.Column(db.Integer())
    gas_avg_daily = db.Column(db.Numeric(19, 2))
    water_avg_daily = db.Column(db.Numeric(19, 2))
    boe_avg_daily = db.Column(db.Numeric(19, 2))
    comments = db.Column(db.JSONB(), nullable=False, server_default="{}")


class ProdStat(Base):
    __tablename__ = "prodstats"

    api10 = db.Column(db.String(10), primary_key=True)
    name = db.Column(db.String(50), primary_key=True, index=True)
    value = db.Column(db.Numeric(19, 2))
    property_name = db.Column(db.String(50), index=True)
    aggregate_type = db.Column(db.String(25), index=True)
    is_peak_norm = db.Column(db.Boolean())
    is_ll_norm = db.Column(db.Boolean())
    ll_norm_value = db.Column(db.Integer())
    includes_zeroes = db.Column(db.Boolean())
    start_date = db.Column(db.Date())
    end_date = db.Column(db.Date())
    start_month = db.Column(db.Integer())
    end_month = db.Column(db.Integer())
    comments = db.Column(db.JSONB(), nullable=False, server_default="{}")

    ix_prodstat_api10_prop_agg = db.Index(
        "ix_prodstat_api10_prop_agg", "api10", "property_name", "aggregate_type"
    )
