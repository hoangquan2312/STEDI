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
AccelerometerLanding_node1699549303607 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1699549303607",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1699549304605 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1699549304605",
)

# Script generated for node SQL Query
SqlQuery0 = """
select 
    distinct c.email, 
    c.customername,
    c.phone, 
    c.birthday, 
    c.serialnumber, 
    c.registrationdate, 
    c.lastupdatedate, 
    c.sharewithresearchasofdate,
    c.sharewithpublicasofdate,
    c.sharewithfriendsasofdate
from c
join a on c.email = a.user;
"""
SQLQuery_node1699549310654 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "c": CustomerTrusted_node1699549304605,
        "a": AccelerometerLanding_node1699549303607,
    },
    transformation_ctx="SQLQuery_node1699549310654",
)

# Script generated for node Customer Curated
CustomerCurated_node1699549315076 = glueContext.getSink(
    path="s3://quannhawsbucket/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerCurated_node1699549315076",
)
CustomerCurated_node1699549315076.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_curated"
)
CustomerCurated_node1699549315076.setFormat("json")
CustomerCurated_node1699549315076.writeFrame(SQLQuery_node1699549310654)
job.commit()
