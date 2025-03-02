import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import re

# Get job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Node: daily_flight_data
daily_flight_data_node1740684335982 = glueContext.create_dynamic_frame.from_catalog(
    database="united_staging_db",
    table_name="raw_daily_flights",
    transformation_ctx="daily_flight_data_node1740684335982"
)

# Node: airport_dim
airport_dim_node1740689745201 = glueContext.create_dynamic_frame.from_catalog(
    database="united_staging_db",
    table_name="united_db_airlines_airports_dim",
    redshift_tmp_dir="s3://united-temp/airline-dim/",
    transformation_ctx="airport_dim_node1740689745201"
)

# Node: Filter
Filter_node1740685392106 = Filter.apply(
    frame=daily_flight_data_node1740684335982,
    f=lambda row: (row["depdelay"] >= 60),
    transformation_ctx="Filter_node1740685392106"
)

# Node: join_dept_airport
Filter_node1740685392106DF = Filter_node1740685392106.toDF()
airport_dim_node1740689745201DF = airport_dim_node1740689745201.toDF()
join_dept_airport_node1740689889723 = DynamicFrame.fromDF(
    Filter_node1740685392106DF.join(
        airport_dim_node1740689745201DF,
        (Filter_node1740685392106DF['originairportid'] == airport_dim_node1740689745201DF['airport_id']),
        "left"
    ),
    glueContext,
    "join_dept_airport_node1740689889723"
)

# Node: modify_dept_airport_columns
modify_dept_airport_columns_node1740689980025 = ApplyMapping.apply(
    frame=join_dept_airport_node1740689889723,
    mappings=[
        ("depdelay", "long", "dep_delay", "bigint"),
        ("arrdelay", "long", "arr_delay", "bigint"),
        ("destairportid", "long", "destairportid", "long"),
        ("carrier", "string", "carrier", "string"),
        ("city", "string", "city", "string"),
        ("name", "string", "name", "string"),
        ("state", "string", "state", "string")
    ],
    transformation_ctx="modify_dept_airport_columns_node1740689980025"
)

# Node: join_arr_airport
modify_dept_airport_columns_node1740689980025DF = modify_dept_airport_columns_node1740689980025.toDF()
join_arr_airport_node1740690278976 = DynamicFrame.fromDF(
    modify_dept_airport_columns_node1740689980025DF.join(
        airport_dim_node1740689745201DF,
        (modify_dept_airport_columns_node1740689980025DF['destairportid'] == airport_dim_node1740689745201DF['airport_id']),
        "left"
    ),
    glueContext,
    "join_arr_airport_node1740690278976"
)

# Node: modify_arr_airport_columns
modify_arr_airport_columns_node1740690540179 = ApplyMapping.apply(
    frame=join_arr_airport_node1740690278976,
    mappings=[
        ("dep_delay", "bigint", "dep_delay", "long"),
        ("arr_delay", "bigint", "arr_delay", "long"),
        ("destairportid", "long", "destairportid", "long"),
        ("carrier", "string", "carrier", "string"),
        ("city", "string", "dep_city", "string"),
        ("name", "string", "name", "string"),
        ("state", "string", "dep_state", "string"),
        ("airport_id", "long", "airport_id", "long")
    ],
    transformation_ctx="modify_arr_airport_columns_node1740690540179"
)

# Node: united_redshift_target
united_redshift_target_node1740690894022 = glueContext.write_dynamic_frame.from_catalog(
    frame=modify_arr_airport_columns_node1740690540179,
    database="united_staging_db",
    table_name="united_db_airlines_daily_flights_fact",
    redshift_tmp_dir="s3://united-temp/airline-fact/",
    additional_options={"aws_iam_role": "arn:aws:iam::741448924843:role/united-redshift-s3-role"},
    transformation_ctx="united_redshift_target_node1740690894022"
)

# Commit Job
job.commit()
