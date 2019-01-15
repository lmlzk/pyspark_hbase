from pyspark.sql import SparkSession
from pyspark_hbase.pyspark_hbase.str2unicode import str2unicode


def connect_hbase(*args, **kwargs):

    appName = kwargs.get("appName")
    host = kwargs.get("host")
    table = kwargs.get("table")
    start_row = kwargs.get("start_row")
    batch = kwargs.get("batch")

    ss = SparkSession.builder.appName(appName).getOrCreate()
    conf = {"hbase.zookeeper.quorum": host, "hbase.mapreduce.inputtable": table}
    conf["TableInputFormat.INPUT_TABLE"] = "hbase.mapreduce.scan"
    if start_row:
        conf["hbase.mapreduce.scan.row.start"] = str2unicode(start_row)
    if batch:
        conf["hbase.mapreduce.scan.row.batchsize"] = batch
    keyConv = "org.apache.spark.examples.pythonconverters.ImmutableBytesWritableToStringConverter"
    valueConv = "org.apache.spark.examples.pythonconverters.HBaseResultToStringConverter"

    hbase_rdd = ss.sparkContext.newAPIHadoopRDD(
        "org.apache.hadoop.hbase.mapreduce.TableInputFormat",
        "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
        "org.apache.hadoop.hbase.client.Result",
        keyConverter=keyConv, valueConverter=valueConv, conf=conf
    )
    return hbase_rdd


if __name__ == '__main__':
    start_row = "\xe1\x00\x1A\x00\x00\x00\x00\x00\x00\x00\x01"
    kwargs = {"appName": "666", "host": 'localhost',
              "table": "t1", "start_row": start_row, "batch": 1}
    rdd = connect_hbase(**kwargs)
    print(rdd.count())