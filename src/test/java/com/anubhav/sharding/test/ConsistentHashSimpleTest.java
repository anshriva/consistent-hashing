package com.anubhav.sharding.test;

import com.anubhav.sharding.database.DaoService;
import com.anubhav.sharding.database.internal.Shard;
import com.anubhav.sharding.database.internal.ShardMapManager;
import com.anubhav.sharding.database.internal.VirtualShard;
import com.anubhav.sharding.hashing.ConsistentHash;
import com.anubhav.sharding.hashing.HashingHelper;
import com.anubhav.sharding.hashing.RecordDo;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConsistentHashSimpleTest {
    @Test
    public void simpleGetAndPutTest(){
        List<Shard> shardList = new ArrayList<>();
        shardList.add(new Shard("A"));
        shardList.add(new Shard("B"));

        List<VirtualShard> virtualShardList = new ArrayList<>();

        virtualShardList.add(new VirtualShard("A0"));
        virtualShardList.add(new VirtualShard("B0"));

        Map<VirtualShard, Shard> virtualShardShardMap = new HashMap<>(){{
            put(virtualShardList.get(0), shardList.get(0));
            put(virtualShardList.get(1), shardList.get(1));
        }};

        Map<Integer, VirtualShard> rangeMaxValueToVirtualShardMap = new HashMap<>(){{
            put(500, virtualShardList.get(0)); // [0, 500]
            put(999, virtualShardList.get(1)); // (500, 999]
        }};
        ShardMapManager shardMapManager = new ShardMapManager(
                shardList,
                virtualShardList,
                virtualShardShardMap,
                rangeMaxValueToVirtualShardMap);

        DaoService daoService = new DaoService(shardMapManager);
        HashingHelper hashingHelper = new HashingHelper();
        ConsistentHash consistentHash = new ConsistentHash(daoService, hashingHelper);

        consistentHash.insert(new RecordDo("1", "one"));
        consistentHash.insert(new RecordDo("600", "six hundred"));
        consistentHash.insert(new RecordDo("999", "nine hundred ninety nine"));
        consistentHash.insert(new RecordDo("0", "zero"));
        consistentHash.insert(new RecordDo("500", "five hundred"));

        RecordDo record = consistentHash.getRecordById("1");
        System.out.println(record.getKey() +" = " + record.getValue());

        record = consistentHash.getRecordById("600");
        System.out.println(record.getKey() +" = " + record.getValue());

        record = consistentHash.getRecordById("999");
        System.out.println(record.getKey() +" = " + record.getValue());

        record = consistentHash.getRecordById("0");
        System.out.println(record.getKey() +" = " + record.getValue());

        record = consistentHash.getRecordById("500");
        System.out.println(record.getKey() +" = " + record.getValue());

        record = consistentHash.getRecordById("1001");
        System.out.println(record !=null ? record.getKey() +" = " + record.getValue(): "null");
    }
}
