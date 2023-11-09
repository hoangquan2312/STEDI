import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Landing
CustomerLanding_node1699545995204 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://quannhawsbucket/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="CustomerLanding_node1699545995204",
)

# Script generated for node Customer Filter
CustomerFilter_node1699546042779 = Filter.apply(
    frame=CustomerLanding_node1699545995204,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="CustomerFilter_node1699546042779",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1699546078902 = glueContext.getSink(
    path="s3://quannhawsbucket/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerTrusted_node1699546078902",
)
CustomerTrusted_node1699546078902.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_trusted"
)
CustomerTrusted_node1699546078902.setFormat("json")
CustomerTrusted_node1699546078902.writeFrame(CustomerFilter_node1699546042779)
job.commit()
