spark-shell --master yarn --conf spark.ui.port=12345 –num-executors 10 –executor-cores 2 –executor-memory 3G –packages com.databricks:spark-avro_2.10:2.0.1


val ordersRDD = sc.textFile("sqoop_import/orders")
case class Orders(order_id: Integer, order_date: String, order_customer_id: Integer, order_status: String)
val ordersDF = ordersRDD.map(rec => rec.split(",")).map(rec => Orders(rec(0).toInt, rec(1), rec(2).toInt, rec(3))).toDF()
ordersDF.registerTempTable("orders_df")	

val orderItemsRDD = sc.textFile("sqoop_import/order_items")
case class OrderItems(order_item_id: Integer, order_item_order_id: Integer, order_item_product_id: Integer, order_item_quantity: Integer, order_item_subtotal: Float, order_item_product_price: Float)
val orderItemsDF = orderItemsRDD.map(rec => rec.split(",")).map(rec => OrderItems(rec(0).toInt, rec(1).toInt, rec(2).toInt, rec(3).toInt, rec(4).toFloat, rec(5).toFloat)).toDF()
orderItemsDF.registerTempTable("order_items_df")

val customersRDD = sc.textFile("sqoop_import/customers")
case class Customers(customer_id:Integer, customer_name:String)
val customersDF = customersRDD.map(rec => rec.split(",")).map(rec => Customers(rec(0).toInt, rec(1).concat(", ").concat(rec(2)))).toDF()
customersDF.registerTempTable("customers_df")

// set number of shuffle partitions to 10
sqlContext.sql("set spark.sql.shuffle.partitions=10");

val fullDataDF = sqlContext.sql("select customers_df.customer_name as name, round(sum(order_items_df.order_item_subtotal),2) as amount from orders_df, order_items_df, customers_df where orders_df.order_id = order_items_df.order_item_id and orders_df.order_customer_id = customers_df.customer_id group by customers_df.customer_name")

// ===================================================================================================================================================================================================================================================================================================================================================================
// WRITE TO TEXT FILE
// save as text file output with pipe as delimiter

fullDataDF.rdd.map(rec => rec.mkString(" | ")).take(10).foreach(println)
fullDataDF.rdd.map(rec => rec.mkString(" | ")).coalesce(1).saveAsTextFile("scala_customer_orders_text_pipe_data")

// save as text file output with tab as delimiter
fullDataDF.rdd.map(rec => rec.mkString(" \t ")).take(10).foreach(println)
fullDataDF.rdd.map(rec => rec.mkString(" \t ")).coalesce(1).saveAsTextFile("scala_customer_orders_text_tab_data")

// save as text file output with comma as delimiter (CSV)
fullDataDF.rdd.map(rec => rec.mkString(" , ")).coalesce(1).saveAsTextFile("scala_customer_orders_text_comma_data")
fullDataDF.rdd.map(rec => rec.mkString(" , ")).coalesce(1).saveAsTextFile("scala_customer_orders_text_comma_data", classOf[org.apache.hadoop.io.compress.SnappyCodec])
 
// ===================================================================================================================================================================================================================================================================================================================================================================
// WRITE TO PARQUET FILE
// save as parquet file with default compression as gz
fullDataDF.write.parquet("scala_customer_orders_parquet")

// save as parquet file with compression as uncompressed
sqlContext.setConf("spark.sql.parquet.compression.codec", "uncompressed")
fullDataDF.write.parquet("scala_customer_orders_parquet_uncomp")

// save as parquet file with compression as snappy
sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
fullDataDF.write.parquet("scala_customer_orders_parquet_snappy")

// READ FROM PARQUET FILE
// file can be read irrespective of compression
val parquetDF = sqlContext.read.parquet("scala_customer_orders_parquet_snappy")
val parquetDF = sqlContext.read.format("parquet").load("scala_customer_orders_parquet_snappy")

// ===================================================================================================================================================================================================================================================================================================================================================================
// WRITE TO JSON FILE
// save as json output file
fullDataDF.toJSON.coalesce(1).saveAsTextFile("scala_customer_orders_json")

// save as json output file with Snappy Compression
fullDataDF.toJSON.coalesce(1).saveAsTextFile("scala_customer_orders_json_snappy", classOf[org.apache.hadoop.io.compress.SnappyCodec])

// READ FROM JSON FILE
// file can be read irrespective of compression
val jsonDF = sqlContext.read.json("scala_customer_orders_json")
val jsonDF = sqlContext.read.format("json").load("scala_customer_orders_json")

// ===================================================================================================================================================================================================================================================================================================================================================================
// WRITE TO ORC FILE
// save as orc file
fullDataDF.write.format("orc").save("scala_customer_orders_orc")

sqlContext.setConf("spark.sql.orc.compression.codec", "snappy")
fullDataDF.write.format("orc").save("scala_customer_orders_orc_snappy")

// READ FROM ORC FILE
// file can be read irrespective of compression
val orcDF = sqlContext.read.orc("scala_customer_orders_orc")
val orcDF = sqlContext.read.format("orc").load("scala_customer_orders_orc")
// ===================================================================================================================================================================================================================================================================================================================================================================
// WRITE TO AVRO FILE
// save as avro file
import com.databricks.spark.avro._;
fullDataDF.write.avro("scala_customer_orders_avro")

sqlContext.setConf("spark.sql.snappy.compression.codec", "snappy")
fullDataDF.write.avro("scala_customer_orders_avro_snappy")

// READ FROM AVRO FILE
// file can be read irrespective of compression
val avroDF = sqlContext.read.avro("scala_customer_orders_avro_snappy")
val avroDF = sqlContext.read.format("com.databricks.spark.avro").load("scala_customer_orders_avro")
// ===================================================================================================================================================================================================================================================================================================================================================================
