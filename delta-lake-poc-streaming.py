# Databricks notebook source
# Define the schema and the input, checkpoint, and output paths.
read_schema = ("n_nationkey int, " +
               "n_name string, " +
               "n_regionkey int, " +
               "n_comment string"
              )
json_read_path = '/FileStore/drop_area/nation_json_files'
checkpoint_path = '/mnt/raw-area/nation/checkpoints'
save_path = '/mnt/raw-area/nation'

people_stream = (spark \
  .readStream \
  .schema(read_schema) \
  .option('maxFilesPerTrigger', 1) \
  .option('multiline', True) \
  .json(json_read_path))

people_stream.writeStream \
  .format('delta') \
  .outputMode('append') \
  .option('checkpointLocation', checkpoint_path) \
  .start(save_path)


# COMMAND ----------


