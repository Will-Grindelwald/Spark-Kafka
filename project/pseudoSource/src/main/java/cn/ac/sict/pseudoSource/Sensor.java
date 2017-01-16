package cn.ac.sict.pseudoSource;

/**
 * 模拟传感器: n 个 温度传感器
 */
public class Sensor {

	private static final String Brokers_List = "master-cent7-1:9092,master-cent7-2:9092,master-cent7-3:9092";

	public static void main(String[] args) {
		if (args.length != 2) {
			System.err.println("Usage: Sensor <topics> <numOfSensor>");
			System.exit(1);
		}

		for (int id = 0; id < Integer.valueOf(args[1]); id++) {
			// 第 i 个温度传感器
			new TemperSensorProducer(Brokers_List, args[0], id).start();
		}
	}

}
