package cs523.SparkStreamingProject

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.hadoop.hbase.{ HBaseConfiguration, HTableDescriptor}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.{ Put, HTable}
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions

object TwitterSpark {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("TwitterSpark")
    val ssc = new StreamingContext(conf, Seconds(5))
    
    val kafkaParams = Map[String, Object](
        elems = "bootstrap.servers" -> "localhost:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "GRP1",
        "auto.offset.reset" -> "latest",
        "enable.auto.commit" -> (false: java.lang.Boolean))
    val topics = Array("twitter_input")
    val kafkaStream = KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams))
    
    val hashTags = kafkaStream.map(record => (record.key(), record.value.toString))
      .flatMap(x => x._2.split(" ").filter(_.startsWith("#")))
      
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.foldLeft(0)(_ + _)
      val previousCount = state.getOrElse(0)
      val updatedSum = currentCount + previousCount
      Some(updatedSum)
    }
    
    ssc.checkpoint("hdfs://quickstart.cloudera:8020/TwitterSpark")
    val count = hashTags.map(x => (x.toLowerCase, 1)).reduceByKey(_+_).updateStateByKey(updateFunc)
    
    def toHbase(row: (_,_)): Unit = {
      val hConf = new HBaseConfiguration()
      hConf.set("hbase.zookeeper.quorum", "localhost:2181")
      val tableName = "TwitterTable"
      val hTable = new HTable(hConf, tableName)
      val tableDescriptor = new HTableDescriptor(tableName)
      val thePut = new Put(Bytes.toBytes(row._1.toString()))
      thePut.add(Bytes.toBytes("HashTag"), Bytes.toBytes("Count"), Bytes.toBytes(row._2.toString()))
      hTable.put(thePut)
    }
    val HBase_inset = count.foreachRDD(rdd => if(!rdd.isEmpty()) rdd.foreach(toHbase(_)))
    
    ssc.start()
    ssc.awaitTermination()
  }
}