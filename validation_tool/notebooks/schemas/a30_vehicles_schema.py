from pandera import DataFrameSchema, Column, Check, Index, MultiIndex

schema = DataFrameSchema(
    columns={
        "Organization": Column(
            dtype="object",
            checks=None,
            nullable=False,
            unique=False,
            coerce=False,
            required=True,
            regex=False,
            description=None,
            title=None,
        ),
        "VIN": Column(
            dtype="object",
            checks=None,
            nullable=False,
            unique=True,
            coerce=False,
            required=True,
            regex=False,
            description=None,
            title=None,
        ),
        "RVI ID": Column(
            dtype="object",
            checks=None,
            nullable=True,
            unique=True,
            coerce=False,
            required=True,
            regex=False,
            description=None,
            title=None,
        ),
        "ADA Accessible Vehicles (0/No 1/Yes)": Column(
            dtype="object",
            checks=None,
            nullable=False,
            unique=False,
            coerce=False,
            required=True,
            regex=False,
            description=None,
            title=None,
        ),
        "Vehicle Type Code": Column(
            dtype="object",
            checks=None,
            nullable=False,
            unique=False,
            coerce=False,
            required=True,
            regex=False,
            description=None,
            title=None,
        ),
        "Funding Source": Column(
            dtype="object",
            checks=None,
            nullable=True,
            unique=False,
            coerce=False,
            required=True,
            regex=False,
            description=None,
            title=None,
        ),
        "Avg. Estimated Service Years When New": Column(
            dtype="float64",
            checks=[
                Check.greater_than_or_equal_to(min_value=8.0),
                Check.less_than_or_equal_to(max_value=14.0),
            ],
            nullable=True,
            unique=False,
            coerce=False,
            required=True,
            regex=False,
            description=None,
            title=None,
        ),
        "Avg. Expected Service Years When New": Column(
            dtype="int64",
            checks=[
                Check.greater_than_or_equal_to(min_value=8.0),
                Check.less_than_or_equal_to(max_value=14.0),
            ],
            nullable=False,
            unique=False,
            coerce=False,
            required=True,
            regex=False,
            description=None,
            title=None,
        ),
        "Year of Manufacture": Column(
            dtype="int64",
            checks=[
                Check.greater_than_or_equal_to(min_value=2002.0),
                Check.less_than_or_equal_to(max_value=2023.0),
            ],
            nullable=False,
            unique=False,
            coerce=False,
            required=True,
            regex=False,
            description=None,
            title=None,
        ),
        "Useful Life Remaining": Column(
            dtype="int64",
            checks=[
                Check.greater_than_or_equal_to(min_value=-16.0),
                Check.less_than_or_equal_to(max_value=22.0),
            ],
            nullable=False,
            unique=False,
            coerce=False,
            required=True,
            regex=False,
            description=None,
            title=None,
        ),
        "Vehicle Length (ft.)": Column(
            dtype="object",
            checks=None,
            nullable=False,
            unique=False,
            coerce=False,
            required=True,
            regex=False,
            description=None,
            title=None,
        ),
        "Seating Capacity": Column(
            dtype="float64",
            checks=[
                Check.greater_than_or_equal_to(min_value=4.0),
                Check.less_than_or_equal_to(max_value=49.0),
            ],
            nullable=True,
            unique=False,
            coerce=False,
            required=True,
            regex=False,
            description=None,
            title=None,
        ),
        "Ownership Type": Column(
            dtype="object",
            checks=None,
            nullable=True,
            unique=False,
            coerce=False,
            required=True,
            regex=False,
            description=None,
            title=None,
        ),
        "Modes Operated": Column(
            dtype="object",
            checks=None,
            nullable=True,
            unique=False,
            coerce=False,
            required=True,
            regex=False,
            description=None,
            title=None,
        ),
    },
    checks=None,
    index=Index(
        dtype="int64",
        checks=[
            Check.greater_than_or_equal_to(min_value=0.0),
            Check.less_than_or_equal_to(max_value=741.0),
        ],
        nullable=False,
        coerce=False,
        name=None,
        description=None,
        title=None,
    ),
    dtype=None,
    coerce=True,
    strict=False,
    name=None,
    ordered=False,
    unique=None,
    report_duplicates="all",
    unique_column_names=False,
    add_missing_columns=False,
    title=None,
    description=None,
)