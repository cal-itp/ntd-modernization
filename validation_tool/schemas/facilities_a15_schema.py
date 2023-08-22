from pandas import Timestamp
from pandera import DataFrameSchema, Column, Check, Index, MultiIndex

schema = DataFrameSchema(
    columns={
        "ReportId": Column(
            dtype="int64",
            checks=[
                Check.greater_than_or_equal_to(min_value=145.0),
                Check.less_than_or_equal_to(max_value=196.0),
            ],
            nullable=False,
            unique=False,
            coerce=False,
            required=True,
            regex=False,
            description=None,
            title=None,
        ),
        "FacilityId": Column(
            dtype="int64",
            checks=[
                Check.greater_than_or_equal_to(min_value=1.0),
                Check.less_than_or_equal_to(max_value=101.0),
            ],
            nullable=False,
            unique=False,
            coerce=False,
            required=True,
            regex=False,
            description=None,
            title=None,
        ),
        "OrganizationId": Column(
            dtype="int64",
            checks=[
                Check.greater_than_or_equal_to(min_value=3663.0),
                Check.less_than_or_equal_to(max_value=3787.0),
            ],
            nullable=False,
            unique=False,
            coerce=False,
            required=True,
            regex=False,
            description=None,
            title=None,
        ),
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
        "GroupPlan": Column(
            dtype="float64",
            checks=None,
            nullable=True,
            unique=False,
            coerce=False,
            required=True,
            regex=False,
            description=None,
            title=None,
        ),
        "FacilityName": Column(
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
        "SectionOfLargerFacility": Column(
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
        "StreetAddress": Column(
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
        "City": Column(
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
        "State": Column(
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
        "ZipCode": Column(
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
        "PrimaryModeServed": Column(
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
        "FacilityTypeCategoryId": Column(
            dtype="int64",
            checks=[
                Check.greater_than_or_equal_to(min_value=1.0),
                Check.less_than_or_equal_to(max_value=20.0),
            ],
            nullable=False,
            unique=False,
            coerce=False,
            required=True,
            regex=False,
            description=None,
            title=None,
        ),
        "FacilityTypeCategory": Column(
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
        "OtherFacilityCategory": Column(
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
        "YearBuilt": Column(
            dtype="int64",
            checks=[
                Check.greater_than_or_equal_to(min_value=1947.0),
                Check.less_than_or_equal_to(max_value=2017.0),
            ],
            nullable=False,
            unique=False,
            coerce=False,
            required=True,
            regex=False,
            description=None,
            title=None,
        ),
        "YearReconstructed": Column(
            dtype="float64",
            checks=[
                Check.greater_than_or_equal_to(min_value=2001.0),
                Check.less_than_or_equal_to(max_value=2001.0),
            ],
            nullable=True,
            unique=False,
            coerce=False,
            required=True,
            regex=False,
            description=None,
            title=None,
        ),
        "UnitSpaceSize": Column(
            dtype="int64",
            checks=[
                Check.greater_than_or_equal_to(min_value=6.0),
                Check.less_than_or_equal_to(max_value=72000.0),
            ],
            nullable=False,
            unique=False,
            coerce=False,
            required=True,
            regex=False,
            description=None,
            title=None,
        ),
        "UnitSpaceType": Column(
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
        "OwnershipType": Column(
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
        "DOTCapitalResponsibility": Column(
            dtype="float64",
            checks=None,
            nullable=True,
            unique=False,
            coerce=False,
            required=True,
            regex=False,
            description=None,
            title=None,
        ),
        "OrganizationCapitalResponsibility": Column(
            dtype="float64",
            checks=[
                Check.greater_than_or_equal_to(min_value=100.0),
                Check.less_than_or_equal_to(max_value=100.0),
            ],
            nullable=True,
            unique=False,
            coerce=False,
            required=True,
            regex=False,
            description=None,
            title=None,
        ),
        "EstimatedConditionAssessment": Column(
            dtype="float64",
            checks=[
                Check.greater_than_or_equal_to(min_value=4.0),
                Check.less_than_or_equal_to(max_value=4.0),
            ],
            nullable=True,
            unique=False,
            coerce=False,
            required=True,
            regex=False,
            description=None,
            title=None,
        ),
        "CurrentCondition": Column(
            dtype="float64",
            checks=[
                Check.greater_than_or_equal_to(min_value=1.5),
                Check.less_than_or_equal_to(max_value=5.0),
            ],
            nullable=False,
            unique=False,
            coerce=False,
            required=True,
            regex=False,
            description=None,
            title=None,
        ),
        "DateOfAssessment": Column(
            dtype="datetime64[ns]",
            checks=[
                Check.greater_than_or_equal_to(
                    min_value=Timestamp("2021-06-30 00:00:00")
                ),
                Check.less_than_or_equal_to(
                    max_value=Timestamp("2021-10-19 00:00:00")
                ),
            ],
            nullable=False,
            unique=False,
            coerce=False,
            required=True,
            regex=False,
            description=None,
            title=None,
        ),
        "Sys_ModifiedUserId": Column(
            dtype="int64",
            checks=[
                Check.greater_than_or_equal_to(min_value=1700.0),
                Check.less_than_or_equal_to(max_value=3417.0),
            ],
            nullable=False,
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
            Check.greater_than_or_equal_to(min_value=164.0),
            Check.less_than_or_equal_to(max_value=232.0),
        ],
        nullable=False,
        coerce=False,
        name="Id",
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