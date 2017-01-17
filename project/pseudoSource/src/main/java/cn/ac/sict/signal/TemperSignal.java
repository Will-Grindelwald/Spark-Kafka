package cn.ac.sict.signal;

/**
 * 温度传感器 自有属性: temper
 *
 */
public class TemperSignal extends Signal {
	public double temper;

	@Override
	public String toString() {
		return super.toString() + "\tTemper: " + temper;
	}
}
