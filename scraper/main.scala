--packages org.apache.spark:spark-avro_2.12:3.0.1

val df = spark.read.format("avro").load("/home/vincent/Code/monolite/scraper/dumb.avro")