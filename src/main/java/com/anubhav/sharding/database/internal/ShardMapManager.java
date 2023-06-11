package com.anubhav.sharding.database.internal;

import java.util.List;
import java.util.Map;

//used to store the information about the shard. The data is supposed to be small
public class ShardMapManager {

    private List<Shard> shardList;

    private List<VirtualShard> virtualShardList;

    // key = virtual shard id , example, shardA0, shardA1
    // value =  actual shard id, example, shardA
    private Map<VirtualShard, Shard> virtualShardToActualShardMapping;

    // key = range, example 150, 150, 300
    // value = server name, example:  serverA1, serverB1, serverA0
    // example serverA1 = [0, 150], serverB1 = (150, 300]
    private Map<Integer, VirtualShard> rangeMaxValueToServerIdMapping;

    public ShardMapManager(List<Shard> shardList, List<VirtualShard> virtualShardList, Map<VirtualShard, Shard> virtualShardToActualShardMapping, Map<Integer, VirtualShard> rangeMaxValueToServerIdMapping) {
        this.shardList = shardList;
        this.virtualShardList = virtualShardList;
        this.virtualShardToActualShardMapping = virtualShardToActualShardMapping;
        this.rangeMaxValueToServerIdMapping = rangeMaxValueToServerIdMapping;
    }

    public Shard getShardById(String shardId){
        return this.shardList.stream().filter(shard -> shard.getShardId().equalsIgnoreCase(shardId)).findFirst().get();
    }

    public Map<Integer, VirtualShard> getRangeMaxValueToServerIdMapping(){
        return this.rangeMaxValueToServerIdMapping;
    }

    public List<Shard> getShardList() {
        return shardList;
    }

    public List<VirtualShard> getVirtualShardList() {
        return virtualShardList;
    }

    public Map<VirtualShard, Shard> getVirtualShardToActualShardMapping() {
        return virtualShardToActualShardMapping;
    }
}
