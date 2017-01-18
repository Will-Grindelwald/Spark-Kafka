package cn.ac.sict.vis;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;

import net.sf.json.JSONObject;
import redis.clients.jedis.Jedis;
import scala.Tuple2;

public class VisMap {
	public static void getKafkaValue(JavaPairReceiverInputDStream<String, String> source, final List list) {

		// kafka value 格式："id":73,"time":1484102241971824,"value":4.3251
		JavaDStream<String> lines = source.map(new Function<Tuple2<String, String>, String>() {
			@Override
			public String call(Tuple2<String, String> tuple2) {
				// 取value值
				return tuple2._2();
			}
		});
		lines.print(280);
		lines.foreachRDD(new VoidFunction<JavaRDD<String>>() {
			@Override
			public void call(JavaRDD<String> t) throws Exception {
				t.foreachPartition(new VoidFunction<Iterator<String>>() {
					@Override
					public void call(Iterator<String> t) throws Exception {
						RedisClient redisClient = new RedisClient();
						Jedis jedis = redisClient.getResource();
						List l = list;
						while (t.hasNext()) {
							String string = t.next();
							JSONObject jasonObject = JSONObject.fromObject(string);
							Map map = (Map) jasonObject;
							int sensorid = (int) map.get("id");
							// 目前数据发送：0-99，数据库编号：1-100
							if (!l.isEmpty() && l.contains(sensorid + 1)) {
								l.remove(l.indexOf(sensorid + 1));
							}
							jedis.set(map.get("id").toString(),
									map.get("temper").toString() + ", time:" + map.get("time").toString());
						}
						// 没有发送数据的站点，此处还应完善
						if (!l.isEmpty()) {
							for (int i = 0; i < l.size(); i++) {
								jedis.publish("error_site", "" + l.get(i));
							}

						}
						redisClient.returnResource(jedis);
					}
				});

			}
		});

	}

}
