package org.czx.impl;

import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.readwritesplitting.algorithm.loadbalance.RoundRobinReplicaLoadBalanceAlgorithm;
import org.apache.shardingsphere.readwritesplitting.spi.ReplicaLoadBalanceAlgorithm;
import org.springframework.util.CollectionUtils;

import java.util.Collection;
import java.util.List;

@Slf4j
public class WfReplicaSelectorAlgorithm implements ReplicaLoadBalanceAlgorithm {
    private ReplicaLoadBalanceAlgorithm delegate;
    public WfReplicaSelectorAlgorithm(){
        delegate = new RoundRobinReplicaLoadBalanceAlgorithm();
    }

    @Override
    public String getDataSource(String name, String writeDataSourceName, List<String> readDataSourceNames) {
        if(!CollectionUtils.isEmpty(readDataSourceNames)) {
            ShardContext shardContext = ShardingContextHolder.get();
            if ((shardContext != null) && !shardContext.isMaster()) {
                log.info("Used read ds");
                return delegate.getDataSource(name, writeDataSourceName, readDataSourceNames);
            }
        }
        log.info("Used write ds");
        return writeDataSourceName;
    }

    @Override
    public String getType() {
        return "WF-REPLICA-SELECTOR";
    }
}
