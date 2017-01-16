package cn.ac.sict.Vis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

//需要序列化
public class RedisClient implements KryoSerializable {

	private JedisPool jedisPool;
	private static final String HOST = "192.168.125.171";

	public RedisClient() {
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxIdle(100);
		jedisPool = new JedisPool(config, HOST);
	}
	
	//调用此方法获取reids连接
	public Jedis getResource() {
		return jedisPool.getResource();
	}

	public void returnResource(Jedis jedis) {
		// jedis.close();
		jedisPool.returnResource(jedis);
	}

	@Override
	public void read(Kryo kryo, Input input) {
		this.jedisPool = new JedisPool(new GenericObjectPoolConfig(), HOST);

	}

	@Override
	public void write(Kryo kryo, Output output) {
		kryo.writeObject(output, HOST);

	}

}
