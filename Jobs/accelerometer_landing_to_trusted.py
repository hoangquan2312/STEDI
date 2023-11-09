import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1699547290486 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1699547290486",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1699547297657 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1699547297657",
)

# Script generated for node SQL Query
SqlQuery109 = """
select * from accelerometer
join customer on accelerometer.user = customer.email

"""
SQLQuery_node1699547302779 = sparkSqlQuery(
    glueContext,
    query=SqlQuery109,
    mapping={
        "customer": CustomerTrusted_node1699547297657,
        "accelerometer": AccelerometerLanding_node1699547290486,
    },
    transformation_ctx="SQLQuery_node1699547302779",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1699547942912 = glueContext.getSink(
    path="s3://quannhawsbucket/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AccelerometerTrusted_node1699547942912",
)
AccelerometerTrusted_node1699547942912.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="accelerometer_trusted"
)
AccelerometerTrusted_node1699547942912.setFormat("json")
AccelerometerTrusted_node1699547942912.writeFrame(SQLQuery_node1699547302779)
job.commit()
