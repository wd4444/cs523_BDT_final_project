package cs523.KafkaSpark;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class KafkaSender {
	private final static String TOPIC = "twitter_input";
    private final static String BOOTSTRAP_SERVERS =
            "localhost:9092";
    
	public static void main(String[] args) throws Exception
	{
		// Create a Java Spark Context
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("KafkaSender").setMaster("local"));

		// Load our input data
		JavaRDD<String> lines = sc.textFile(args[0]);
		
		// Simulate Twitter API
		// Send to Kafka line by line
		runProducer(5, lines.collect());

		sc.close();
	}
	
	private static Producer<Long, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                            BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                                        LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                                    StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }
	
	static void runProducer(final int sendMessageCount, final List<String> list) throws Exception {
	      final Producer<Long, String> producer = createProducer();
	      long time = System.currentTimeMillis();

	      try {
	          for (int index = 0; index < list.size(); index++) {
	        	  Thread.sleep((long)(Math.random() * ((8000 - 3000) + 1)));
	              final ProducerRecord<Long, String> record =
	                      new ProducerRecord<>(TOPIC, System.currentTimeMillis() + index, list.get(index));

	              RecordMetadata metadata = producer.send(record).get();

	              long elapsedTime = System.currentTimeMillis() - time;
	              System.out.printf("sent record(key=%s value=%s) " +
	                              "meta(partition=%d, offset=%d) time=%d\n",
	                      record.key(), record.value(), metadata.partition(),
	                      metadata.offset(), elapsedTime);
	              
	    	  }
	      } finally {
	          producer.flush();
	          producer.close();
	      }
	  }

}
