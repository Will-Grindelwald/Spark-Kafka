package ljg;

public class Sensors {
		public static void main(String[] args) {
			    int sensorNumber = Integer.parseInt(args[0] );
			    long timeStamp = System.currentTimeMillis()/1000;
				SensorFactory.getSensors(sensorNumber, timeStamp);
				SensorFactory.start();
}
}