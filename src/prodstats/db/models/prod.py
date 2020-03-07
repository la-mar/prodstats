from db.models.bases import Base, db

__all__ = ["ProdMonthly", "ProdStat"]


class ProdMonthly(Base):
    __tablename__ = "production_monthly"

    api10 = db.Column(db.String(10), primary_key=True)
    prod_date = db.Column(db.Date())
    oil = db.Column(db.Numeric(19, 2))
    gas = db.Column(db.Numeric(19, 2))
    water = db.Column(db.Numeric(19, 2))
    gas_norm_5k_ft = db.Column(db.Numeric(19, 2))
    oil_norm_5k_ft = db.Column(db.Numeric(19, 2))
    gas_norm_7500_ft = db.Column(db.Numeric(19, 2))
    oil_norm_7500_ft = db.Column(db.Numeric(19, 2))
    gas_norm_10k_ft = db.Column(db.Numeric(19, 2))
    oil_norm_10k_ft = db.Column(db.Numeric(19, 2))
    prod_day = db.Column(db.Numeric(19, 2))
    prod_month = db.Column(db.Integer())
    peak_norm_month = db.Column(db.Integer())


class ProdHeader(Base):
    __tablename__ = "prodstat_header"

    api10 = db.Column(db.String(10), primary_key=True)
    api14s = db.Column(db.ARRAY(db.Integer), default=[])
    prod_date_first = db.Column(db.Date())
    prod_date_last = db.Column(db.Date())
    days_on = db.Column(db.Integer())
    monthsproducing = db.Column(db.Integer())


class ProdStatDef(Base):
    __tablename__ = "prodstat_definitions"

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    # data_model = db.Column(db.String(50), nullable=False)  # header, monthly
    name = db.Column(db.String(50), index=True, unique=True, nullable=False)
    uom = db.Column(db.String(50))
    agg_type = db.Column(db.String(50))  # avg, sum, peak, etc
    stat_type = db.Column(db.String(50))  # oil, gas, water, gor, etc
    stat_subtype = db.Column(db.String(50))  # peak30, daily, total,
    start_month = db.Column(db.Integer())  # null means "unbounded"
    end_month = db.Column(db.Integer())  # null means "unbounded"
    norm_value = db.Column(db.Float())  # norm by the perfed lateral length
    norm_uom = db.Column(db.String(25))  # ft only
    include_zero = db.Column(db.Boolean())

    description = db.Column(db.String())

    # model_name_uix = db.UniqueConstraint("data_model", "name", name="model_name_uix")


class ProdStat(Base):
    __tablename__ = "prodstats"

    # id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    api10 = db.Column(db.String(10), primary_key=True)
    prodstat_def_id = db.Column(
        db.ForeignKey("prodstat_definitions.id"), primary_key=True
    )

    value = db.Column(db.Float())
    prod_date_first = db.Column(db.Date())
    prod_date_last = db.Column(db.Date())
    prod_month_first = db.Column(db.Integer())
    prod_month_last = db.Column(db.Integer())
    comments = db.Column(db.String())

    # api10_prodstat_def_id_unique = db.UniqueConstraint("api10", "prodstat_def_id")
    # gor_first6mo = db.Column(db.Integer())
    # gor_first6mo_nonzero = db.Column(db.Integer())  # gor_first6mononzero
    # oil_percent_first6mo = db.Column(db.Integer())
    # oil_percent_last3mo = db.Column(db.Integer())
    # oil_pk30 = db.Column(db.Integer())
    # oil_pk30_date = db.Column(db.Date())
    # oil_p30_prodmonth = db.Column(db.Integer())
    # oil_avgdaily_last3mo = db.Column(db.Integer())
    # oil_pdp_30kpbbl_last3mo = db.Column(db.Integer())
    # oil_pknorm_perk_1mo = db.Column(db.Integer())
    # oil_pknorm_perk_3mo = db.Column(db.Integer())
    # oil_pknorm_perk_6mo = db.Column(db.Integer())
    # oil_total = db.Column(db.Integer())
    # oil_perk_first1mo = db.Column(db.Integer())
    # oil_perk_first3mo = db.Column(db.Integer())
    # oil_perk_first6mo = db.Column(db.Integer())
    # oil_perk_first9mo = db.Column(db.Integer())
    # oil_perk_first12mo = db.Column(db.Integer())
    # oil_perk_first18mo = db.Column(db.Integer())
    # oil_perk_first24mo = db.Column(db.Integer())
    # oil_sum_last1mo = db.Column(db.Integer())
    # oil_sum_last3mo = db.Column(db.Integer())
    # oil_sum_last3mo_nonzero = db.Column(db.Integer())  # oil_sumnonzero_last3mo
    # oil_sum_first1mo = db.Column(db.Integer())
    # oil_sum_first3mo = db.Column(db.Integer())
    # oil_sum_first6mo = db.Column(db.Integer())
    # oil_sum_first9mo = db.Column(db.Integer())
    # oil_sum_first12mo = db.Column(db.Integer())
    # oil_sum_first18mo = db.Column(db.Integer())
    # oil_sum_first24mo = db.Column(db.Integer())
    # oil_sum_pknorm_1mo = db.Column(db.Integer())
    # oil_sum_pknorm_3mo = db.Column(db.Integer())
    # oil_sum_pknorm_6mo = db.Column(db.Integer())
    # gas_pk30 = db.Column(db.Integer())
    # gas_avgdaily_last3mo = db.Column(db.Integer())
    # gas_pknorm_perk_1mo = db.Column(db.Integer())
    # gas_pknorm_perk_3mo = db.Column(db.Integer())
    # gas_pknorm_perk_6mo = db.Column(db.Integer())
    # gas_total = db.Column(db.Integer())
    # gas_perk_first1mo = db.Column(db.Integer())
    # gas_perk_first3mo = db.Column(db.Integer())
    # gas_perk_first6mo = db.Column(db.Integer())
    # gas_perk_first9mo = db.Column(db.Integer())
    # gas_perk_first12mo = db.Column(db.Integer())
    # gas_perk_first18mo = db.Column(db.Integer())
    # gas_perk_first24mo = db.Column(db.Integer())
    # gas_sum_last1mo = db.Column(db.Integer())
    # gas_sum_last3mo = db.Column(db.Integer())
    # gas_sum_last3mo_nonzero = db.Column(db.Integer())  # gas_sumnonzero_last3mo
    # gas_sum_first1mo = db.Column(db.Integer())
    # gas_sum_first3mo = db.Column(db.Integer())
    # gas_sum_first6mo = db.Column(db.Integer())
    # gas_sum_first9mo = db.Column(db.Integer())
    # gas_sum_first12mo = db.Column(db.Integer())
    # gas_sum_first18mo = db.Column(db.Integer())
    # gas_sum_first24mo = db.Column(db.Integer())
    # gas_sum_pknorm_1mo = db.Column(db.Integer())
    # gas_sum_pknorm_3mo = db.Column(db.Integer())
    # gas_sum_pknorm_6mo = db.Column(db.Integer())
    # boe_avgdaily_last3mo = db.Column(db.Integer())
    # boe_pdp_30kpbbl_last3mo = db.Column(db.Integer())
    # water_sum_last1mo = db.Column(db.Integer())
    # water_sum_last3mo = db.Column(db.Integer())
    # water_sum_last3mo_nonzero = db.Column(db.Integer())
    # productiondatecutoff = db.Column(db.Date())
