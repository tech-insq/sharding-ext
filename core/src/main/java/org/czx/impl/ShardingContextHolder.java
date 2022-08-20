package org.czx.impl;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ShardingContextHolder {
    private final static ThreadLocal<ShardContext> contextHolder = new ThreadLocal<>();
    public static ShardContext create(){
        ShardContext shardContext = new ShardContext();
        contextHolder.set(shardContext);
        return shardContext;
    }

    public static ShardContext get(){
        return contextHolder.get();
    }

    public static void clear(){
        contextHolder.remove();
    }
}
