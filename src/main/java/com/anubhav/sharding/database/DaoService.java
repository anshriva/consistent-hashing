package com.anubhav.sharding.database;

import com.anubhav.sharding.database.internal.Record;
import com.anubhav.sharding.database.internal.Shard;
import com.anubhav.sharding.database.internal.ShardMapManager;
import com.anubhav.sharding.database.internal.VirtualShard;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class DaoService {
    private ShardMapManager shardMapManager;

    public DaoService(ShardMapManager shardMapManager) {
        this.shardMapManager = shardMapManager;
    }

    public List<Record> getRecordsBySharedIdAndVirtualShardId(String shardId, String virtualShardId) {
        Shard shardDetails = this.shardMapManager.getShardById(shardId);
        List<Record> recordsForVirtualShardId = shardDetails.
                getRecords().
                stream().
                filter(record -> record.getVirtualShardKey().equalsIgnoreCase(virtualShardId)).
                collect(Collectors.toList());
        return recordsForVirtualShardId;
    }

    public void addRecordToShard(String shardId, Record record) {
        Shard shard = this.shardMapManager.getShardById(shardId);
        shard.getRecords().add(record);
    }

    public void removeRecordFromShard(String shardId, String id) {
        Shard shard = this.shardMapManager.getShardById(shardId);
        Optional<Record> recordToBeRemoved = shard.
                getRecords().
                stream().
                filter(r -> r.getKey().equalsIgnoreCase(id)).
                findFirst();
        if (recordToBeRemoved.isPresent()) {
            shard.getRecords().remove(recordToBeRemoved.get());
        } else {
            System.out.println("record to be removed is not present. id = " + id + "in shard id = " + shardId);
        }
    }

    public int[] getRangesAssignedInAscendingOrder() {
        var rangeToServerIdMapping = this.shardMapManager.getRangeMaxValueToServerIdMapping();
        var keySet = rangeToServerIdMapping.keySet();
        int[] serverMappings = new int[keySet.size()];
        int i = 0;
        for (var x : keySet) {
            serverMappings[i] = x;
            i++;
        }
        Arrays.sort(serverMappings);
        return serverMappings;
    }

    public Shard getShardFromEndRange(int endValueInRange) {
        VirtualShard virtualShard = this.getVirtualShardFromEndRange(endValueInRange);
        return this.shardMapManager.getVirtualShardToActualShardMapping().get(virtualShard);
    }


    public VirtualShard getVirtualShardFromEndRange(int endValueInRange) {
        var rangeToServerIdMapping = this.shardMapManager.getRangeMaxValueToServerIdMapping();
        return rangeToServerIdMapping.get(endValueInRange);
    }
}
