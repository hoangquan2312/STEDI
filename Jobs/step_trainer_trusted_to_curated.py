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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1699553294309 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="StepTrainerTrusted_node1699553294309",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1699553294823 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node1699553294823",
)

# Script generated for node Privacy Filter
SqlQuery175 = """
select s.sensorreadingtime, s.serialnumber, s.distancefromobject, a.user, a.x, a.y, a.z
from step_trainer_trusted s
join accelerometer_trusted a on a.timestamp = s.sensorreadingtime;
"""
PrivacyFilter_node1699553301631 = sparkSqlQuery(
    glueContext,
    query=SqlQuery175,
    mapping={
        "accelerometer_trusted": AccelerometerTrusted_node1699553294823,
        "step_trainer_trusted": StepTrainerTrusted_node1699553294309,
    },
    transformation_ctx="PrivacyFilter_node1699553301631",
)

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1699553304186 = glueContext.getSink(
    path="s3://quannhawsbucket/step_trainer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="MachineLearningCurated_node1699553304186",
)
MachineLearningCurated_node1699553304186.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="machine_learning_curated"
)
MachineLearningCurated_node1699553304186.setFormat("json")
MachineLearningCurated_node1699553304186.writeFrame(PrivacyFilter_node1699553301631)
job.commit()
