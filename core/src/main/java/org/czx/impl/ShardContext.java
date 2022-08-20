package org.czx.impl;

import lombok.extern.slf4j.Slf4j;
import org.czx.vo.RwRole;

@Slf4j
public class ShardContext {
    private RwRole rwRole;
    private boolean isLocalTxnCheck;
    private String localDs;
    public ShardContext(){
        rwRole = RwRole.Master;
        isLocalTxnCheck = false;
        localDs = null;
    }

    public void setLocalTxnCheck(boolean localTxnCheck) {
        isLocalTxnCheck = localTxnCheck;
    }

    public void setRwRole(RwRole rwRole) {
        this.rwRole = rwRole;
    }

    public boolean isMaster(){
        return (rwRole.compareTo(RwRole.Master) == 0);
    }

    public void checkLocalTxn(String ds){
        if(!isLocalTxnCheck){
            return;
        }
        if(localDs == null){
            localDs = ds;
            return;
        }
        if(!localDs.equals(ds)){
            log.warn("Check Local txn failed: localDs={}, cure ds={}", localDs, ds);
        }
    }
}
