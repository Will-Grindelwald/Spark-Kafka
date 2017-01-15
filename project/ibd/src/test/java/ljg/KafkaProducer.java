package ljg;

 

import java.util.List;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducer {
	  public static final String METADATA_BROKER_LIST_KEY = "metadata.broker.list";
	  public static final String SERIALIZER_CLASS_KEY = "serializer.class";
	  public static final String SERIALIZER_CLASS_VALUE = "kafka.serializer.StringEncoder";
	  
	  private static KafkaProducer insantce = null;
	  private Producer producer;
	  private KafkaProducer (String brokerList){
		  
		   Properties properties = new Properties();
		   properties.put(METADATA_BROKER_LIST_KEY, brokerList );
		   properties.put(SERIALIZER_CLASS_KEY, SERIALIZER_CLASS_VALUE);
		   properties.put("kafka.message.CompressionCodec", "1");
		   properties.put("client.id", "streaming-kafka-output");
		   ProducerConfig producerConfig = new ProducerConfig(properties);
		   this.producer = new Producer(producerConfig);
	  }
	  public static synchronized KafkaProducer getInstance(String brokerList){
		   if(insantce == null){
			   insantce = new KafkaProducer(brokerList);
			   System.out.println("初始化 producer");
		   }
		   return insantce;
		  
	  }
	  
	  public void send(KeyedMessage<String, String> keyedMessage){
		  producer.send(keyedMessage);
	  }
	  
	  public void send(List<KeyedMessage<String, String>>keyedMessageList){
		  producer.send(keyedMessageList);
	  }
	  
	  public void shutdown(){
		  producer.close();
	  }
}
