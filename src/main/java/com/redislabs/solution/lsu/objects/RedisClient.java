package com.redislabs.solution.lsu.objects;

import java.util.List;

/**
 * Created by foo on 7/25/15.
 */
public interface RedisClient {

    // sets up the client
    void setup(String host, int port);

    // does a pipelined hset
    void hset(List<byte[]> keys, List<byte[]> hashKeys, List<byte[]> hashValues);

    // does a pipelined hget
    List<Object> hget(List<byte[]> keys, List<byte[]> hashKeys);

}
