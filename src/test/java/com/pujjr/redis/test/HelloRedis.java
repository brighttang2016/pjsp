package com.pujjr.redis.test;

import java.util.List;
import java.util.Set;

import redis.clients.jedis.Jedis;

public class HelloRedis {

	public static void main(String[] args) {
		System.out.println("redis 测试");
//		Jedis jedis = new Jedis("112.74.136.25");
		Jedis jedis = new Jedis("172.18.10.50");
		System.out.println(jedis.ping());
		String str = jedis.get("username");
		System.out.println(str);
		Set<String> set = jedis.smembers("bbs");
		//set
//		jedis.set("password", "123456");
		System.out.println(jedis.get("password"));
		
		//list
		
		List<String> list = jedis.lrange("mylist", 0, 4);
		
	}

}
