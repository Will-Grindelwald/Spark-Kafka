package ljg;
import java.util.Arrays;
import  java.util.regex.Pattern;

import org.apache.log4j.chainsaw.Main;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;
/***
 * 
 *SparkStreaming 对终端输入的单词进行计数
 *
 */
public class JavaNetworkWordCount {
		
	private static final  Pattern Space = Pattern.compile(" ");
	public static void main(String[] args) throws InterruptedException {
		SparkConf sparkConf = new SparkConf().setAppName("WordCount");
		JavaStreamingContext jsc = new JavaStreamingContext(sparkConf,Durations.seconds(1) );
		int port = Integer.parseInt(args[0]);
		JavaReceiverInputDStream<String> lines = jsc.socketTextStream("192.168.125.171", port);
		JavaDStream<String> words = lines.flatMap(
				new FlatMapFunction<String, String>() {
					@Override
					 public java.util.Iterator<String> call(String x) throws Exception {
						 return Arrays.asList(Space.split(x)).iterator();
					 
					 };
					 }
				);
	   JavaPairDStream<String , Integer> wordsCounts = words.mapToPair(
				new	PairFunction<String,String,Integer>() {
					@Override
					 public Tuple2<String,Integer> call(String s) throws Exception {
						 
						 return new Tuple2<>(s, 1);
					 }
				}
		).reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer arg0, Integer arg1) throws Exception {
				// TODO Auto-generated method stub
				return  arg0+arg1;
			}
		});
	    wordsCounts.print();
	    jsc.start();
	    jsc.awaitTermination();
	}
	
	
	
}
