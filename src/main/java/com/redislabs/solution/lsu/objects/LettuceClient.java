package com.redislabs.solution.lsu.objects;

import com.google.common.collect.Lists;
import com.lambdaworks.redis.*;
import java.util.List;

/**
 * Created by foo on 7/25/15.
 */
// todo: wait for v3.3 (end of August) or find a way to use the snapshot
public class LettuceClient implements RedisClient {

    // to use pipelining one must not use the pool :(

    private com.lambdaworks.redis.RedisClient client;
//    private com.lambdaworks.redis.RedisConnectionPool<RedisConnection<String, String>> pool;

    @Override
    public void setup(String host, int port) {
        client = new com.lambdaworks.redis.RedisClient(host, port);
    }

    @Override
    public void hset(List<byte[]> keys, List<byte[]> hashKeys, List<byte[]> hashValues) {
        RedisAsyncConnection<String, String> c = client.connectAsync();
        // c.setAutoFlushCommands(false)....
        List<RedisFuture<?>> futures = Lists.newArrayList();
    }

    @Override
    public List<Object> hget(List<byte[]> keys, List<byte[]> hashKeys) {
        return null;
    }
}
