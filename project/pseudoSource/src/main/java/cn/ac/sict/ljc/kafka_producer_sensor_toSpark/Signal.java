package cn.ac.sict.ljc.kafka_producer_sensor_toSpark;

public abstract class Signal {
	private long id;
	private long time;

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}
}
