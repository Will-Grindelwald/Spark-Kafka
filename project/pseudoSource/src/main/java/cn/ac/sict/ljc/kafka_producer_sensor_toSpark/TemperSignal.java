package cn.ac.sict.ljc.kafka_producer_sensor_toSpark;

public class TemperSignal extends Signal {
	private double value;

	public double getValue() {
		return value;
	}

	public void setValue(double value) {
		this.value = value;
	}
}
