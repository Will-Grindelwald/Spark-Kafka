package ljg;

import java.util.Random;

import org.json.JSONObject;
/***
 * 
 *  模拟传感器向Kafka发送数据流
 *
 */
public class SensorSimulator {
	private int  id ;
	private long startTimeStamp;

	 
	
	public SensorSimulator(int id, long startTimeStamp) {
		super();
		this.id = id;
		this.startTimeStamp = startTimeStamp;
	}
	public  void  sendDataStream( ) {
		
		String bootstrapServers = "192.168.125.171:9092,192.168.125.172:9092,192.168.125.173:9092";
	    KafkaProducerDemo producerTest = new KafkaProducerDemo(bootstrapServers);
		 Random r =new Random();
	    //for(int i =0;i<10000;i++)
		 while(true){
			 
		    JSONObject msg = new JSONObject();
		    msg.put("sensor_id",id);
		    msg.put("time_stamp",  startTimeStamp+=1 );
		 	msg.put("temperature",r.nextInt(50) );
			System.out.println(msg.toString());
		    producerTest.sendMessage( "origin-topic",msg.toString());
	}
	}
	public static void main(String[] args) {
		long timeStamp = System.currentTimeMillis()/1000;
		SensorSimulator s =new SensorSimulator(1,timeStamp );
		s.sendDataStream();
	}
}
