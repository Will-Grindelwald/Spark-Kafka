package cn.ac.sict.signal;

/**
 * 所有传感器信号的父类, 以后的新的传感器信号必须继承此类
 * 
 */
public abstract class Signal {
	public long id;
	public long time;

	@Override
	public String toString() {
		return "ID: " + id + "\tTime: " + time;
	}
}
