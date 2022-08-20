package org.czx.impl;

import com.google.common.base.Preconditions;
import lombok.Generated;
import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.sharding.api.sharding.ShardingAutoTableAlgorithm;
import org.apache.shardingsphere.sharding.api.sharding.standard.PreciseShardingValue;
import org.apache.shardingsphere.sharding.api.sharding.standard.RangeShardingValue;
import org.apache.shardingsphere.sharding.api.sharding.standard.StandardShardingAlgorithm;
import org.springframework.util.StringUtils;

import java.util.Collection;
import java.util.Properties;

@Slf4j
public class WfUidShardingAlg implements StandardShardingAlgorithm<Comparable<?>>, ShardingAutoTableAlgorithm {
    private static final String SHARDING_COUNT_KEY = "sharding-count";
    private static final String SHARDING_DS_TYPE = "sharding-ds-mode";
    private Properties props = new Properties();
    private int shardingCount;
    private boolean forDs;

    @Override
    public int getAutoTablesAmount() {
        return shardingCount;
    }

    @Override
    public Collection<String> getAllPropertyKeys() {
        return null;
    }

    @Override
    public String doSharding(Collection<String> collection, PreciseShardingValue<Comparable<?>> shardingValue) {
        String suffix = String.valueOf(this.toShardingValue(shardingValue.getValue()) % this.shardingCount);
        String ds = this.findMatchedTargetName(collection, suffix, shardingValue.getDataNodeInfo()).orElse(null);
        if(forDs){
            ShardContext shardContext = ShardingContextHolder.get();
            if(shardContext != null){
                shardContext.checkLocalTxn(ds);
            }
        }
        return ds;
    }

    @Override
    public Collection<String> doSharding(Collection<String> collection,
                                         RangeShardingValue<Comparable<?>> rangeShardingValue) {
        return collection;
    }

    @Override
    public void init() {
        this.shardingCount = this.getShardingCount();
        this.forDs = this.verifyIsForDs();
    }

    @Override
    public String getType() {
        return "WF-UID-MOD";
    }

    @Generated
    public Properties getProps() {
        return this.props;
    }

    @Generated
    public void setProps(Properties props) {
        this.props = props;
    }

    @Generated
    public void setShardingCount(int shardingCount) {
        this.shardingCount = shardingCount;
    }

    private int toShardingValue(Comparable<?> shardingValue){
        String value = (String) shardingValue;
        if(StringUtils.isEmpty(value) || (value.length() <= 2)){
            return 0;
        }
        String subValue = value.substring(value.length() - 2);
        try{
            return Integer.parseInt(subValue, 16);
        }catch (Throwable t) {
            log.warn("shardingValue={},subValue={} to sharding value exceptions:{}", value,
                    subValue, t.getMessage());
            return 0;
        }
    }

    private int getShardingCount() {
        Preconditions.checkArgument(this.props.containsKey(SHARDING_COUNT_KEY), "Sharding count cannot be null.");
        return Integer.parseInt(this.props.get(SHARDING_COUNT_KEY).toString());
    }

    private boolean verifyIsForDs(){
        String mode = this.props.getProperty(SHARDING_DS_TYPE, "table");
        return "ds".equals(mode);
    }
}
