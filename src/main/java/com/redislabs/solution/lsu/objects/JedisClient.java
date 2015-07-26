package com.redislabs.solution.lsu.objects;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;

import java.util.List;

/**
 * Created by Guy on 12/14/2014.
 */

// add lettuce for better performance
public class JedisClient implements RedisClient {

    JedisPool pool;

    public JedisClient(String host, int port) {
        setup(host, port);
    }

    @Override
    public void setup(String host, int port) {

        JedisPoolConfig poolConfig=new JedisPoolConfig();

        // defaults to make your life with connection pool easier :)
        poolConfig.setTestWhileIdle(false);
        poolConfig.setTestOnBorrow(false);
        poolConfig.setTestOnReturn(false);
        poolConfig.setMinEvictableIdleTimeMillis(60000);
        poolConfig.setTimeBetweenEvictionRunsMillis(30000);
        poolConfig.setNumTestsPerEvictionRun(-1);
        poolConfig.setMaxTotal(500);

        pool = new JedisPool(poolConfig, host , port,10000);

    }

    public void set(String key, String value) {

        Jedis jedis = pool.getResource();

        jedis.set(key, value);

        pool.returnResource(jedis);

    }

    public String get(String key) {

        Jedis jedis = pool.getResource();

        String val =  jedis.get(key);

        pool.returnResource(jedis);

        return val;
    }

    public List<String> mGet(String[] keys) {

        Jedis jedis = pool.getResource();

        List<String> res =  jedis.mget(keys);

        pool.returnResource(jedis);

        

        return res;

    }

    public void mSet(String[] keys, String[] values) {


        Jedis jedis = pool.getResource();

        if (keys.length == 10)
            jedis.mset(keys[0],values[0],keys[1],values[1],keys[2],values[2],keys[3],values[3],keys[4],values[4],
                keys[5],values[5],keys[6],values[6],keys[7],values[7],keys[8],values[8],keys[9],values[9]);
        else if ( keys.length == 25 )
            jedis.mset(keys[0],values[0],keys[1],values[1],keys[2],values[2],keys[3],values[3],keys[4],values[4],
                    keys[5],values[5],keys[6],values[6],keys[7],values[7],keys[8],values[8],keys[9],values[9],
                    keys[10],values[10],keys[11],values[11],keys[12],values[12],keys[13],values[13],keys[14],values[14],
                    keys[15],values[15],keys[16],values[16],keys[17],values[17],
                    keys[18],values[18],keys[19],values[19],keys[20],values[20],keys[21],values[21],keys[22],values[22],
                    keys[23],values[23],keys[24],values[24]);



        pool.returnResource(jedis);


    }

    public void flush() {
        Jedis jedis = pool.getResource();
        jedis.flushAll();
        pool.returnResource(jedis);
    }


    public void hset(String key, String hashKey, String hashValue) {
        Jedis jedis = pool.getResource();
        jedis.hset(key, hashKey, hashValue);
        pool.returnResource(jedis);
    }

    public String hget(String key, String hashKey) {

        Jedis jedis = pool.getResource();
        String result =  jedis.hget(key, hashKey);
        pool.returnResource(jedis);
        return result;

    }

    @Override
    public void hset(List<byte[]> keys, List<byte[]> hashKeys, List<byte[]> hashValues) {

        try {
            Jedis jedis = pool.getResource();
            Pipeline pipeline = jedis.pipelined();

            for (int i = 0; i < keys.size(); i++) {
                pipeline.hset(keys.get(i), hashKeys.get(i), hashValues.get(i));
            }

            pipeline.sync();
            pool.returnResource(jedis);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<Object> hget(List<byte[]> keys, List<byte[]> hashKeys) {

        Jedis jedis = pool.getResource();
        Pipeline pipeline = jedis.pipelined();

        for (int i = 0; i < keys.size(); i++) {
            pipeline.hget(keys.get(i), hashKeys.get(i));
        }

        List<Object> results = pipeline.syncAndReturnAll();
        pool.returnResource(jedis);

        return results;
    }
}
