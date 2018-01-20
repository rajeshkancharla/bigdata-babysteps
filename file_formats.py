pyspark --master yarn --conf spark.ui.port=12345 –num-executors 10 –executor-cores 2 –executor-memory 3G –packages com.databricks:spark-avro_2.10:2.0.1
pyspark --master yarn --num-executors 10 --packages com.databricks:spark-avro_2.10:2.0.1

from pyspark import Row

ordersRDD = sc.textFile("sqoop_import/orders")
ordersDF = ordersRDD.map(lambda rec: Row(order_id = int(rec.split(",")[0]), order_date = rec.split(",")[1], order_customer_id = 
                                         int(rec.split(",")[2]), order_status = rec.split(",")[3])).toDF()
ordersDF.registerTempTable("orders_df")

orderItemsRDD = sc.textFile("sqoop_import/order_items")
orderItemsDF = orderItemsRDD.map(lambda rec: Row(order_item_id = int(rec.split(",")[0]), order_item_order_id = int(rec.split(",")[1]), 
                                                 order_item_product_id = int(rec.split(",")[2]), order_item_quantity = 
                                                 int(rec.split(",")[3]), order_item_subtotal = float(rec.split(",")[4]), 
                                                 order_item_product_price = float(rec.split(",")[5]))).toDF()
orderItemsDF.registerTempTable("order_items_df")

customersRDD = sc.textFile("sqoop_import/customers")
customersDF = customersRDD.map(lambda rec: Row(customer_id = int(rec.split(",")[0]), customer_name = rec.split(",")[1] + ", " + 
                                               rec.split(",")[2])).toDF()
customersDF.registerTempTable("customers_df")

# set number of shuffle partitions to 10
sqlContext.sql("set spark.sql.shuffle.partitions=10");

fullDataDF = sqlContext.sql("select customers_df.customer_name as name, round(sum(order_items_df.order_item_subtotal),2) as amount from 
                            orders_df, order_items_df, customers_df where orders_df.order_id = order_items_df.order_item_id and 
                            orders_df.order_customer_id = customers_df.customer_id group by customers_df.customer_name")

# ===================================================================================================================================================================================================================================================================================================================================================================
# WRITE TO TEXT FILE
# save as text file output with tab as delimiter
fullDataDF.rdd.map(lambda rec: "\t".join([str(x) for x in rec])).coalesce(1).saveAsTextFile("pyspark_customer_orders_text_tab_data")

# save as text file output with pipe as delimiter
fullDataDF.rdd.map(lambda rec: "|".join([str(x) for x in rec])).coalesce(1).saveAsTextFile("customer_orders_text_pipe_data")

# save as text file output with pipe as delimiter
fullDataDF.rdd.map(lambda rec: ",".join([str(x) for x in rec])).coalesce(1).saveAsTextFile("customer_orders_text_csv_data", 
                                                                                           "org.apache.hadoop.io.compress.SnappyCodec")


fullDataRDD = sc.textFile("pyspark_customer_orders_text_tab_data")
fullDataMap = fullDataRDD.map(lambda rec: (rec.split("\t")[0], rec.split("\t")[1]))
# ===================================================================================================================================================================================================================================================================================================================================================================
# WRITE TO PARQUET FILE
# save as parquet file with default compression as gz
fullDataDF.write.parquet("customer_orders_parquet")

# save as parquet file with compression as uncompressed
sqlContext.setConf("spark.sql.parquet.compression.codec", "uncompressed")
fullDataDF.write.parquet("customer_orders_parquet_uncomp")

# save as parquet file with compression as snappy
sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
fullDataDF.write.parquet("customer_orders_parquet_snappy")

# READ FROM PARQUET FILE
# file can be read irrespective of compression
parquetDF = sqlContext.read.parquet("customer_orders_parquet_uncomp")
parquetDF = sqlContext.read.format("parquet").load("customer_orders_parquet_uncomp")

# ===================================================================================================================================================================================================================================================================================================================================================================
# WRITE TO JSON FILE
# save as json output file
fullDataDF.write.format("json").save("pyspark_customer_orders_direct_json")

fullDataDF.toJSON().coalesce(1).saveAsTextFile("customer_orders_json")

# save as json output file with Snappy Compression
fullDataDF.toJSON().coalesce(1).saveAsTextFile("customer_orders_json_snappy", "org.apache.hadoop.io.compress.SnappyCodec")

# READ FROM JSON FILE
# file can be read irrespective of compression
jsonDF = sqlContext.read.json("customer_orders_json_snappy")
jsonDF = sqlContext.read.format("json").load("customer_orders_json_snappy")

# ===================================================================================================================================================================================================================================================================================================================================================================
# WRITE TO ORC FILE
# save as orc file
fullDataDF.write.format("orc").save("customer_orders_orc")

sqlContext.setConf("spark.sql.orc.compression.codec", "snappy")
fullDataDF.write.format("orc").save("customer_orders_orc_snappy")

# READ FROM ORC FILE
# file can be read irrespective of compression
orcDF = sqlContext.read.orc("customer_orders_orc_snappy")
orcDF = sqlContext.read.format("orc").load("customer_orders_orc_snappy")

# ===================================================================================================================================================================================================================================================================================================================================================================
# WRITE TO AVRO FILE
# save as avro file
# import com.databricks.spark.avro._;
fullDataDF.write.format("com.databricks.spark.avro").save("customer_orders_avro")

sqlContext.setConf("spark.sql.snappy.compression.codec", "snappy")
fullDataDF.write.format("com.databricks.spark.avro").save("customer_orders_avro_snappy")

# READ FROM AVRO FILE
# file can be read irrespective of compression
avroDF = sqlContext.read.format("com.databricks.spark.avro").load("customer_orders_avro_snappy")

# ===================================================================================================================================================================================================================================================================================================================================================================

# SEQUENCE
# save as sequence file
fullDataDF.rdd.saveAsSequenceFile("customer_orders_sequence")




