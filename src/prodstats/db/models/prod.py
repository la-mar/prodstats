from db.models.bases import Base, db

__all__ = ["ProdMonthly", "ProdStat"]


class ProdMonthly(Base):
    __tablename__ = "production_monthly"

    api10 = db.Column(db.String(10), primary_key=True)
    prod_date = db.Column(db.Date())
    prod_month = db.Column(db.Integer())
    days_in_month = db.Column(db.Integer())
    prod_days = db.Column(db.Integer())
    peak_norm_month = db.Column(db.Integer())
    peak_norm_days = db.Column(db.Integer())  # currently prod_day
    oil = db.Column(db.Numeric(19, 2))
    gas = db.Column(db.Numeric(19, 2))
    water = db.Column(db.Numeric(19, 2))
    boe = db.Column(db.Numeric(19, 2))
    oil_norm_1k = db.Column(db.Numeric(19, 2))
    gas_norm_1k = db.Column(db.Numeric(19, 2))
    boe_norm_1k = db.Column(db.Numeric(19, 2))
    gas_norm_5k = db.Column(db.Numeric(19, 2))
    oil_norm_5k = db.Column(db.Numeric(19, 2))
    boe_norm_5k = db.Column(db.Numeric(19, 2))
    gas_norm_7500 = db.Column(db.Numeric(19, 2))
    oil_norm_7500 = db.Column(db.Numeric(19, 2))
    boe_norm_7500 = db.Column(db.Numeric(19, 2))
    gas_norm_10k = db.Column(db.Numeric(19, 2))
    oil_norm_10k = db.Column(db.Numeric(19, 2))
    boe_norm_10k = db.Column(db.Numeric(19, 2))
    oil_percent = db.Column(db.Float())
    gor = db.Column(db.Float())


class ProdHeader(Base):
    __tablename__ = "prodstat_header"

    api10 = db.Column(db.String(10), primary_key=True)
    api14s = db.Column(db.ARRAY(db.Integer), default=[])
    prod_date_first = db.Column(db.Date())
    prod_date_last = db.Column(db.Date())
    prod_months = db.Column(db.Integer())  # months producing
    prod_days = db.Column(db.Integer())
    peak_norm_months = db.Column(db.Integer())  # months producing
    peak_norm_days = db.Column(db.Integer())
    peak30_oil = db.Column(db.Integer())
    peak30_gas = db.Column(db.Integer())
    peak30_date = db.Column(db.Date())
    peak30_month = db.Column(db.Integer())


# class ProdStatDef(Base):
#     __tablename__ = "prodstat_definitions"

#     id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
#     # data_model = db.Column(db.String(50), nullable=False)  # header, monthly
#     name = db.Column(db.String(50), index=True, unique=True, nullable=False)
#     uom = db.Column(db.String(50))
#     agg_type = db.Column(db.String(50))  # avg, sum, peak, etc
#     stat_type = db.Column(
#         db.String(50)
#     )  # name of callable returning series of monthly values
#     stat_subtype = db.Column(db.String(50))  # peak30, daily, total,
#     start_month = db.Column(db.Integer())  # null means "unbounded"
#     end_month = db.Column(db.Integer())  # null means "unbounded"
#     use_peak_norm = db.Column(db.Boolean())
#     norm_by = db.Column(db.Float())  # norm by the perfed lateral length
#     norm_by_uom = db.Column(db.String(25))  # ft only
#     include_zero = db.Column(db.Boolean())

#     description = db.Column(db.String())

# model_name_uix = db.UniqueConstraint("data_model", "name", name="model_name_uix")


class ProdStat(Base):
    __tablename__ = "prodstats"

    # id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    api10 = db.Column(db.String(10), primary_key=True)
    name = db.Column(db.String(50), primary_key=True)
    value = db.Column(db.Float())
    property_name = db.Column(db.String(50))
    aggregate_type = db.Column(db.String(25))
    is_peak_normalized = db.Column(db.Boolean())
    is_lateral_length_normalized = db.Column(db.Boolean())
    includes_zeroes = db.Column(db.Boolean())
    start_date = db.Column(db.Date())
    end_date = db.Column(db.Date())
    start_month = db.Column(db.Integer())
    end_month = db.Column(db.Integer())
    comments = db.Column(db.String())

    # api10_prodstat_def_id_unique = db.UniqueConstraint("api10", "prodstat_def_id")
    # gor_first6mo = db.Column(db.Integer())
    # gor_first6mo_nonzero = db.Column(db.Integer())  # gor_first6mononzero
    # oil_percent_first6mo = db.Column(db.Integer())
    # oil_percent_last3mo = db.Column(db.Integer())
    # oil_pk30 = db.Column(db.Integer())
    # oil_pk30_date = db.Column(db.Date())
    # oil_pk30_prodmonth = db.Column(db.Integer())
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
