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

public class ConsistentHashNoVirtualNodeTest {

    // initially we will have 3 nodes and the distribution will be as follows
    // A : [0, 333]
    // B : (333, 666]
    // C : (666, 999]
    // The range is from [0,1000]
    // For simplicity, we have hash("number") = number
    // we will assign shard id from id = 0 to 1000
    // then we will add one more node from [0, 160]
    // then we will recalculate the hash to see the number of re-balances needed
    @Test
    public void no_VirtualNode_CheckNumberOfRebalancedWhenNodeAdded(){
        List<Shard> shardList = new ArrayList<>();
        shardList.add(new Shard("A"));
        shardList.add(new Shard("B"));
        shardList.add(new Shard("C"));

        List<VirtualShard> virtualShardList = new ArrayList<>();

        virtualShardList.add(new VirtualShard("A0"));
        virtualShardList.add(new VirtualShard("B0"));
        virtualShardList.add(new VirtualShard("C0"));

        Map<VirtualShard, Shard> virtualShardShardMap = new HashMap<>(){{
            put(virtualShardList.get(0), shardList.get(0));
            put(virtualShardList.get(1), shardList.get(1));
            put(virtualShardList.get(2), shardList.get(2));
        }};

        Map<Integer, VirtualShard> rangeMaxValueToVirtualShardMap = new HashMap<>(){{
            put(333, virtualShardList.get(0)); // [0, 333]
            put(666, virtualShardList.get(1)); // (333, 666]
            put(999, virtualShardList.get(2)); // (666, 999]
        }};

        ShardMapManager shardMapManager = new ShardMapManager(
                shardList,
                virtualShardList,
                virtualShardShardMap,
                rangeMaxValueToVirtualShardMap);

        DaoService daoService = new DaoService(shardMapManager);
        HashingHelper hashingHelper = new HashingHelper();
        ConsistentHash consistentHash = new ConsistentHash(daoService, hashingHelper);

        for(int i=0;i<1000;i++){
            consistentHash.insert(new RecordDo(String.valueOf(i), String.valueOf(i)));
        }

        var shardD = new Shard("D");
        shardList.add(shardD);
        var virtualShardD = new VirtualShard("D0");
        virtualShardList.add(virtualShardD);
        virtualShardShardMap.put(virtualShardD, shardD);
        rangeMaxValueToVirtualShardMap.put(160, virtualShardD);


        int numberOfMovementsNeeded = consistentHash.ReBalance(shardD);

        System.out.println("number of movement needed = "+ numberOfMovementsNeeded);
        System.out.println(consistentHash.getRecordById("1").getValue());
        System.out.println(consistentHash.getRecordById("160").getValue());
        System.out.println(consistentHash.getRecordById("161").getValue());
        System.out.println(consistentHash.getRecordById("162").getValue());

    }
}
