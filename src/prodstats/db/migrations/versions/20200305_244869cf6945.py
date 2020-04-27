"""create extensions

Revision ID: 244869cf6945
Revises:
Create Date: 2020-03-05 18:59:05.382421+00:00

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "244869cf6945"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():

    with op.get_bind().engine.connect().execution_options(
        isolation_level="AUTOCOMMIT"
    ) as conn:
        try:
            conn.execute("create extension if not exists postgis;")
        except Exception:
            print(
                """Unable to create extension postgis.  If running on RDS, this must be
                done manually. See https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Appendix.PostgreSQL.CommonDBATasks.html#Appendix.PostgreSQL.CommonDBATasks.PostGIS for details. """  # noqa
            )


def downgrade():
    with op.get_bind().engine.connect().execution_options(
        isolation_level="AUTOCOMMIT"
    ) as conn:
        try:
            conn.execute("drop extension if not exists postgis;")
        except Exception:
            print(
                """Unable to create extension postgis.  If running on RDS, this must be
                done manually. See https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Appendix.PostgreSQL.CommonDBATasks.html#Appendix.PostgreSQL.CommonDBATasks.PostGIS for details. """  # noqa
            )
