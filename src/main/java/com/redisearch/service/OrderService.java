package com.redisearch.service;


import com.redisearch.model.Order;
import com.redisearch.repository.OrderRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class OrderService {

    @Autowired
    private OrderRepository orderRepo;

    public List<Order> search(String searchKey) {

        return orderRepo.search(searchKey);
    }
}
