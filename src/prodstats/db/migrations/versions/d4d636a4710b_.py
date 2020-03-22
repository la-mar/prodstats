"""empty message

Revision ID: d4d636a4710b
Revises: 03d10d7b15e7
Create Date: 2020-03-21 19:00:56.495709

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "d4d636a4710b"
down_revision = "03d10d7b15e7"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        "frac_parameters",
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
        sa.Column("fluid_bbl", sa.Integer(), nullable=True),
        sa.Column("proppant_lb", sa.Integer(), nullable=True),
        sa.Column("fluid_bbl_ft", sa.Integer(), nullable=True),
        sa.Column("proppant_lb_ft", sa.Integer(), nullable=True),
        sa.Column("gen_int", sa.Integer(), nullable=True),
        sa.Column("gen_str", sa.String(length=10), nullable=True),
        sa.PrimaryKeyConstraint("api14"),
    )
    op.create_index(
        op.f("ix_frac_parameters_api14"), "frac_parameters", ["api14"], unique=False
    )
    op.add_column("shapes", sa.Column("bhllat", sa.Float(), nullable=True))
    op.add_column("shapes", sa.Column("bhllon", sa.Float(), nullable=True))
    op.add_column("shapes", sa.Column("shllat", sa.Float(), nullable=True))
    op.add_column("shapes", sa.Column("shllon", sa.Float(), nullable=True))
    op.drop_index("idx_shapes_bent_stick", table_name="shapes")
    op.drop_index("idx_shapes_bent_stick_webmercator", table_name="shapes")
    op.drop_index("idx_shapes_bhl", table_name="shapes")
    op.drop_index("idx_shapes_bhl_webmercator", table_name="shapes")
    op.drop_index("idx_shapes_heel", table_name="shapes")
    op.drop_index("idx_shapes_heel_webmercator", table_name="shapes")
    op.drop_index("idx_shapes_kop", table_name="shapes")
    op.drop_index("idx_shapes_kop_webmercator", table_name="shapes")
    op.drop_index("idx_shapes_lateral_only", table_name="shapes")
    op.drop_index("idx_shapes_lateral_only_webmercator", table_name="shapes")
    op.drop_index("idx_shapes_mid", table_name="shapes")
    op.drop_index("idx_shapes_mid_webmercator", table_name="shapes")
    op.drop_index("idx_shapes_shl", table_name="shapes")
    op.drop_index("idx_shapes_shl_webmercator", table_name="shapes")
    op.drop_index("idx_shapes_stick", table_name="shapes")
    op.drop_index("idx_shapes_stick_webmercator", table_name="shapes")
    op.drop_index("idx_shapes_toe", table_name="shapes")
    op.drop_index("idx_shapes_toe_webmercator", table_name="shapes")
    op.drop_index("idx_shapes_wellbore", table_name="shapes")
    op.drop_index("idx_shapes_wellbore_webmercator", table_name="shapes")
    op.drop_column("wells", "bhllat")
    op.drop_column("wells", "bhllon")
    op.drop_column("wells", "frac_bbl_ft")
    op.drop_column("wells", "frac_gen_int")
    op.drop_column("wells", "primary_product")
    op.drop_column("wells", "frac_lb")
    op.drop_column("wells", "frac_gen_str")
    op.drop_column("wells", "frac_lb_ft")
    op.drop_column("wells", "shllat")
    op.drop_column("wells", "shllon")
    op.drop_column("wells", "abandoned_date")
    op.drop_column("wells", "frac_bbl")
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column(
        "wells", sa.Column("frac_bbl", sa.INTEGER(), autoincrement=False, nullable=True)
    )
    op.add_column(
        "wells",
        sa.Column("abandoned_date", sa.DATE(), autoincrement=False, nullable=True),
    )
    op.add_column(
        "wells",
        sa.Column(
            "shllon",
            postgresql.DOUBLE_PRECISION(precision=53),
            autoincrement=False,
            nullable=True,
        ),
    )
    op.add_column(
        "wells",
        sa.Column(
            "shllat",
            postgresql.DOUBLE_PRECISION(precision=53),
            autoincrement=False,
            nullable=True,
        ),
    )
    op.add_column(
        "wells",
        sa.Column("frac_lb_ft", sa.INTEGER(), autoincrement=False, nullable=True),
    )
    op.add_column(
        "wells",
        sa.Column("frac_gen_str", sa.VARCHAR(), autoincrement=False, nullable=True),
    )
    op.add_column(
        "wells", sa.Column("frac_lb", sa.INTEGER(), autoincrement=False, nullable=True)
    )
    op.add_column(
        "wells",
        sa.Column(
            "primary_product", sa.VARCHAR(length=10), autoincrement=False, nullable=True
        ),
    )
    op.add_column(
        "wells",
        sa.Column("frac_gen_int", sa.INTEGER(), autoincrement=False, nullable=True),
    )
    op.add_column(
        "wells",
        sa.Column("frac_bbl_ft", sa.INTEGER(), autoincrement=False, nullable=True),
    )
    op.add_column(
        "wells",
        sa.Column(
            "bhllon",
            postgresql.DOUBLE_PRECISION(precision=53),
            autoincrement=False,
            nullable=True,
        ),
    )
    op.add_column(
        "wells",
        sa.Column(
            "bhllat",
            postgresql.DOUBLE_PRECISION(precision=53),
            autoincrement=False,
            nullable=True,
        ),
    )
    op.create_index(
        "idx_shapes_wellbore_webmercator",
        "shapes",
        ["wellbore_webmercator"],
        unique=False,
    )
    op.create_index("idx_shapes_wellbore", "shapes", ["wellbore"], unique=False)
    op.create_index(
        "idx_shapes_toe_webmercator", "shapes", ["toe_webmercator"], unique=False
    )
    op.create_index("idx_shapes_toe", "shapes", ["toe"], unique=False)
    op.create_index(
        "idx_shapes_stick_webmercator", "shapes", ["stick_webmercator"], unique=False
    )
    op.create_index("idx_shapes_stick", "shapes", ["stick"], unique=False)
    op.create_index(
        "idx_shapes_shl_webmercator", "shapes", ["shl_webmercator"], unique=False
    )
    op.create_index("idx_shapes_shl", "shapes", ["shl"], unique=False)
    op.create_index(
        "idx_shapes_mid_webmercator", "shapes", ["mid_webmercator"], unique=False
    )
    op.create_index("idx_shapes_mid", "shapes", ["mid"], unique=False)
    op.create_index(
        "idx_shapes_lateral_only_webmercator",
        "shapes",
        ["lateral_only_webmercator"],
        unique=False,
    )
    op.create_index("idx_shapes_lateral_only", "shapes", ["lateral_only"], unique=False)
    op.create_index(
        "idx_shapes_kop_webmercator", "shapes", ["kop_webmercator"], unique=False
    )
    op.create_index("idx_shapes_kop", "shapes", ["kop"], unique=False)
    op.create_index(
        "idx_shapes_heel_webmercator", "shapes", ["heel_webmercator"], unique=False
    )
    op.create_index("idx_shapes_heel", "shapes", ["heel"], unique=False)
    op.create_index(
        "idx_shapes_bhl_webmercator", "shapes", ["bhl_webmercator"], unique=False
    )
    op.create_index("idx_shapes_bhl", "shapes", ["bhl"], unique=False)
    op.create_index(
        "idx_shapes_bent_stick_webmercator",
        "shapes",
        ["bent_stick_webmercator"],
        unique=False,
    )
    op.create_index("idx_shapes_bent_stick", "shapes", ["bent_stick"], unique=False)
    op.drop_column("shapes", "shllon")
    op.drop_column("shapes", "shllat")
    op.drop_column("shapes", "bhllon")
    op.drop_column("shapes", "bhllat")
    op.drop_index(op.f("ix_frac_parameters_api14"), table_name="frac_parameters")
    op.drop_table("frac_parameters")
    # ### end Alembic commands ###