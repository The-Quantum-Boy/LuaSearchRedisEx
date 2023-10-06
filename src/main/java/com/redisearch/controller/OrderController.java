package com.redisearch.controller;


import com.redisearch.model.Order;
import com.redisearch.service.OrderService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/")
public class OrderController {

    @Autowired
    private OrderService orderService;


    @GetMapping("/search")
    public List<Order> search(
            @RequestParam(name = "searchKey",required = false) String  searchKey
    ){
        return orderService.search(searchKey);
    }
}
