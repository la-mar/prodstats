from db.models.bases import Base, db

__all__ = ["ProdMonthly"]


class ProdMonthly(Base):
    __tablename__ = "production_monthly"

    api10 = db.Column(db.String(10), index=True, primary_key=True)
    prod_date = db.Column(db.Date())
    oil = db.Column(db.Numeric(19, 2))
    gas = db.Column(db.Numeric(19, 2))
    water = db.Column(db.Numeric(19, 2))
    gas_normalized_to_10k_ft = db.Column(db.Numeric(19, 2))
    oil_normalized_to_10k_ft = db.Column(db.Numeric(19, 2))
    gas_normalized_to_7500_ft = db.Column(db.Numeric(19, 2))
    oil_normalized_to_7500_ft = db.Column(db.Numeric(19, 2))
    gas_normalized_to_5k_ft = db.Column(db.Numeric(19, 2))
    oil_normalized_to_5k_ft = db.Column(db.Numeric(19, 2))
    prod_day = db.Column(db.Numeric(19, 2))
    prod_month = db.Column(db.Integer())
    peak_norm_month = db.Column(db.Integer())
