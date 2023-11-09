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

# Script generated for node Customer Curated
CustomerCurated_node1699549860311 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_curated",
    transformation_ctx="CustomerCurated_node1699549860311",
)

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1699549859417 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_landing",
    transformation_ctx="StepTrainerLanding_node1699549859417",
)

# Script generated for node Customer Filter
SqlQuery117 = """
select s.sensorreadingtime, s.serialnumber, s.distancefromobject 
from s
join c on c.serialnumber = s.serialnumber;

"""
CustomerFilter_node1699549865310 = sparkSqlQuery(
    glueContext,
    query=SqlQuery117,
    mapping={
        "c": CustomerCurated_node1699549860311,
        "s": StepTrainerLanding_node1699549859417,
    },
    transformation_ctx="CustomerFilter_node1699549865310",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1699549868181 = glueContext.getSink(
    path="s3://quannhawsbucket/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="StepTrainerTrusted_node1699549868181",
)
StepTrainerTrusted_node1699549868181.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="step_trainer_trusted"
)
StepTrainerTrusted_node1699549868181.setFormat("json")
StepTrainerTrusted_node1699549868181.writeFrame(CustomerFilter_node1699549865310)
job.commit()
