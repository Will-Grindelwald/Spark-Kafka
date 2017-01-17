package ljg;
/***
 * 创建Kafka生产者，向指定topic发送消息
 */
import java.util.Properties;
import java.util.Random;
import org.json.JSONObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
public class KafkaProducerDemo {
	private  final Producer<String, String> producer;
	private  String bootstrapServers;
	public KafkaProducerDemo( String servers) {
		 bootstrapServers=servers;
		 Properties props =new Properties();
		 props.put("bootstrap.servers", bootstrapServers);
		 props.put("batch.size", 16384);
		 props.put("linger.ms",1);
		 props.put("buffer.memory", 33554432);
		 props.put("acks", "all");
		 props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		 props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		 producer = new KafkaProducer<String, String>(props);
	 
	}
	public void sendMessage(String topic,  int  messageCount    ){
	   Random r =new Random();
	  
	   long timeStamp = System.currentTimeMillis()/1000;
	   
	   while(true){
		   long startTime = System.currentTimeMillis();
		    String randomNumber = Integer.toString(r.nextInt(50));
		    JSONObject msg = new JSONObject();
		    msg.put("time", timeStamp+=1 );
		 	msg.put("number",randomNumber   );
		    producer.send(new ProducerRecord<String, String>(topic, msg.toString()));
		    long endTime = System.currentTimeMillis();
		    try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	   	}
 
	}
	public void sendMessage(String topic, String message ){
		producer.send(new ProducerRecord<String, String>(topic, message));
	}
	
   public static void main(String[] args) {
	   String bootstrapServers = "192.168.125.171:9092,192.168.125.172:9092,192.168.125.173:9092";
	   KafkaProducerDemo producerTest = new KafkaProducerDemo(bootstrapServers);
	   String topic = args[0];
	   int messageCount = Integer.parseInt(args[1]);
	   producerTest.sendMessage(topic, messageCount);
}
}
