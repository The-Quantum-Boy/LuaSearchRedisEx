package com.redisearch.config;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.exceptions.JedisDataException;

@Configuration
@Data
public class RedisConfiguration {

    @Value("classpath:search.lua")
    Resource luaScript;

    @Bean
    JedisPooled jedisPooled() {
        return new JedisPooled("localhost", 6379);
    }

    @Bean
    String loadScript() throws Exception {
        String data = new String(luaScript.getInputStream().readAllBytes());
        System.out.println("Loaded Lua Script:\n" + data);
        return data;
    }



}
