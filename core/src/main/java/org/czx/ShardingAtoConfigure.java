package org.czx;

import org.czx.aop.ShardingAspect;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class ShardingAtoConfigure {
    @Bean
    public ShardingAspect createShardingAspect(){
        return new ShardingAspect();
    }
}
