package cn.ac.sict.pseudoSource;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.google.gson.Gson;

import cn.ac.sict.signal.TemperSignal;

public class TemperSensorProducer extends Thread {

	private static final String partitioner = SimplePartitioner.class.getName();
	private static final int min = 30, max = 70;

	private final int ID; // 组号, 用于分区
	private final String Topic;
	private final Producer<String, String> producer;

	private Random rnd;
	private double randValue;
	private TemperSignal signal;

	public TemperSensorProducer(String kafkaStr, String topic, int id) {
		Properties props = new Properties();
		props.put("bootstrap.servers", kafkaStr);
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", StringSerializer.class.getName());
		props.put("value.serializer", StringSerializer.class.getName());
		props.put("partitioner.class", partitioner);

		this.ID = id;
		this.Topic = topic;
		this.producer = new KafkaProducer<String, String>(props);

		this.rnd = new Random(System.nanoTime());
		this.randValue = (double) (min + rnd.nextInt((max - min) * 1000) / 1000.0);
		this.signal = new TemperSignal();
		signal.id = ID;
	}

	@Override
	public void run() {
		try {
			String key = (ID) + "", msg = null; // key = ID
			Gson gson = new Gson();
			while (true) {
				nextValue(signal);
				msg = gson.toJson(signal);
				ProducerRecord<String, String> newRecord = new ProducerRecord<String, String>(Topic, key, msg);
				// System.out.println(ID + "\t1");
				producer.send(newRecord);
				// System.out.println(ID + "\t2");
				System.out.println(msg);

				// 1000 ms 发一次消息
				try {
					Thread.sleep(999);
				} catch (InterruptedException e) {
				}
			}
		} finally {
			this.producer.close();
		}
	}

	private void nextValue(TemperSignal signal) {
		int direct = -1;
		if (rnd.nextInt() % 2 == 0)
			direct *= -1;
		randValue = randValue + direct * rnd.nextInt(1000) * rnd.nextGaussian() / 1000; // 下一个尽量连续的随机数
		while (randValue > max)
			randValue -= rnd.nextDouble() / 100;
		while (randValue < min)
			randValue += rnd.nextDouble() / 100;
		signal.temper = (int) (randValue * 10000) / 10000.0; // 精度为 4 位小数
		signal.time = new Date().getTime() * 1000 + System.nanoTime() % 1000000 / 1000;
	}

}
