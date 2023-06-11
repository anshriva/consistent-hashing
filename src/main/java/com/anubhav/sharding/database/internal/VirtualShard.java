package com.anubhav.sharding.database.internal;

public class VirtualShard {
    private String virtualShardKey;

    public VirtualShard(String virtualShardKey) {
        this.virtualShardKey = virtualShardKey;
    }

    public String getVirtualShardKey() {
        return virtualShardKey;
    }

    public void setVirtualShardKey(String virtualShardKey) {
        this.virtualShardKey = virtualShardKey;
    }
}
