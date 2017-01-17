package ljg;
/***
 * 
 * @author 定义线程，启动传感器
 *
 */
public class SendMessage implements Runnable {
    private long timeStamp;
    private   int sensorId;
	@Override
	public void run() {
		SensorSimulator  sensorSimulator = new SensorSimulator(sensorId , timeStamp);
		sensorSimulator.sendDataStream();
	}
	public SendMessage(int sensorId, long timeStamp) {
		 
		this.sensorId = sensorId;
		this.timeStamp = timeStamp;   
	}
			
}
