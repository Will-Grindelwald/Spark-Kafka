package ljg;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import jersey.repackaged.com.google.common.collect.Lists;
import kafka.producer.KeyedMessage;
import kafka.serializer.StringDecoder;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import scala.Tuple2;
/****
 * 实现Kafka与Spark Streaming集成：
 * 1.数据从kafka流入
 * 2.SparkStreaming处理
 * 3.处理后的数据流发送到Kafka,redis
 */
public class KafkaSparkStreaming {
	public static final String KAFKA_GROUP_ID = "kafka-streaming";
	public static void main(String[] args) {
		//Kafka 与Spark Streaming 集成
		String topic = "origin";//通过Kafka将消息发送到该主题
		HashSet<String> topicSet = new HashSet<>();
		topicSet.add(topic);
	    HashMap<String, String> kafkaParam = new HashMap<>();
	    kafkaParam.put("metadata.broker.list" ,"master-cent7-1:9092,master-cent7-2:9092,master-cent7-3:9092");
	    kafkaParam.put("group.id",KAFKA_GROUP_ID);
	    SparkConf sparkConf = new SparkConf().setAppName("streaming-kafka");
	    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf,new Duration(5000) );
	    // 通过Direct方式创建Dstream
	    JavaPairDStream<String, String> kafkaPairDStream = KafkaUtils.createDirectStream(jssc,String.class, String.class,StringDecoder.class,StringDecoder.class ,kafkaParam,topicSet);
	    
	    JavaDStream<String> message = kafkaPairDStream.map( 
	    		new Function<Tuple2<String,String>,  String>() {
	    			public String call(Tuple2<String, String>v1) throws  Exception {
	    				return v1._2();
	    			}
				}	
	    		);
	    //该部分可以编写业务逻辑
	    
	    
	    // 通过遍历RDD来获取Kafka发送的每条消息
	     message.foreachRDD(new VoidFunction<JavaRDD<String>>() {
			 @Override
			 public void call(JavaRDD<String> v1) throws Exception {
				 v1.foreachPartition( new VoidFunction<Iterator<String>>() {
					 @Override
			 public void call(Iterator<String> stringIterator ) throws Exception { 
				String bootstrapServers = "192.168.125.171:9092,192.168.125.172:9092,192.168.125.173:9092";
			    KafkaProducerDemo producerTest = new KafkaProducerDemo(bootstrapServers);
			    JedisPool pool =new  JedisPool(new JedisPoolConfig(),"192.168.125.171");
			    try(Jedis jRedis = pool.getResource() ){
			    while(stringIterator.hasNext()){
			   
				producerTest.sendMessage( "dest",stringIterator.next());//将RDD中的数据发送到dest主题	
				jRedis.publish("messages" , stringIterator.next() );//将RDD数据发送到redis
			    }			 
			    } catch (Exception e) {
					 e.printStackTrace( );
				}    
			    pool.close();
					}
			    });
		   }
	    });
	     jssc.start();
	     try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
