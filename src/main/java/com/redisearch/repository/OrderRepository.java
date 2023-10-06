package com.redisearch.repository;


import ch.qos.logback.core.net.server.Client;
import com.google.gson.Gson;
import com.redisearch.model.Order;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Repository;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.search.Document;
import redis.clients.jedis.search.Query;
import redis.clients.jedis.search.SearchResult;

import java.util.*;
import java.util.stream.Collectors;

@Repository
public class OrderRepository {

    @Autowired
    private JedisPooled jedis;


    @Autowired
    private String luaScript;



    public static final Integer PAGE_SIZE = 5;

    public Order save(Order order) {
        Gson gson = new Gson();
        String key = order.getInternalOrdNo();
        jedis.hset("orders", key, gson.toJson(order));
        return order;
    }


    public void deleteAll() {
        Map<String, String> orders = jedis.hgetAll("orders");
        if (!orders.isEmpty()) {
            List<String> orderKeys = new ArrayList<>(orders.keySet());
            jedis.hdel("orders", orderKeys.toArray(new String[0]));

        }
    }


    public List<Order> search(String searchKey) {
        try {
            System.out.println("search"+System.currentTimeMillis());
            String searchScript = jedis.scriptLoad(luaScript, "searchscript");
            Object result = jedis.evalsha(searchScript, 1, "orders", searchKey);
            System.out.println("search"+System.currentTimeMillis());

            List<String> results = (List<String>) result;

            List<Order> orders = new ArrayList<>();
            for (String json : results) {
                Gson gson = new Gson();
                Order order = gson.fromJson(json, Order.class);
                orders.add(order);
            }

            System.out.println(orders);
            return orders;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }


}
