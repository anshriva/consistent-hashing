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

public class ConsistentHashVirtualNodeTest {
    @Test
    public void VirtualNode_CheckNumberOfRebalancedWhenNodeAdded(){
        List<Shard> shardList = new ArrayList<>();
        shardList.add(new Shard("A"));
        shardList.add(new Shard("B"));
        shardList.add(new Shard("C"));

        List<VirtualShard> virtualShardList = new ArrayList<>();

        virtualShardList.add(new VirtualShard("A0"));
        virtualShardList.add(new VirtualShard("A1"));
        virtualShardList.add(new VirtualShard("A2"));

        virtualShardList.add(new VirtualShard("B0"));
        virtualShardList.add(new VirtualShard("B1"));
        virtualShardList.add(new VirtualShard("B2"));

        virtualShardList.add(new VirtualShard("C0"));
        virtualShardList.add(new VirtualShard("C1"));
        virtualShardList.add(new VirtualShard("C2"));

        Map<VirtualShard, Shard> virtualShardShardMap = new HashMap<>(){{
            put(virtualShardList.get(0), shardList.get(0));
            put(virtualShardList.get(1), shardList.get(0));
            put(virtualShardList.get(2), shardList.get(0));

            put(virtualShardList.get(3), shardList.get(1));
            put(virtualShardList.get(4), shardList.get(1));
            put(virtualShardList.get(5), shardList.get(1));

            put(virtualShardList.get(6), shardList.get(2));
            put(virtualShardList.get(7), shardList.get(2));
            put(virtualShardList.get(8), shardList.get(2));
        }};

        Map<Integer, VirtualShard> rangeMaxValueToVirtualShardMap = new HashMap<>(){{
            put(111, virtualShardList.get(0)); // [0, 111]
            put(222, virtualShardList.get(3)); // (0, 222]
            put(333, virtualShardList.get(6)); // (0, 333]
            put(444, virtualShardList.get(1)); // (0, 444]
            put(555, virtualShardList.get(4)); // (0, 555]
            put(666, virtualShardList.get(7)); // (0, 666]
            put(777, virtualShardList.get(2)); // (0, 777]
            put(888, virtualShardList.get(5)); // (0, 888]
            put(999, virtualShardList.get(8)); // (0, 999]
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

        // Add a new shard

        var shardD = new Shard("D");
        var virtualShardD0 = new VirtualShard("D0");
        var virtualShardD1 = new VirtualShard("D1");
        var virtualShardD2 = new VirtualShard("D2");

        shardList.add(shardD);
        virtualShardList.add(virtualShardD0);
        virtualShardList.add(virtualShardD1);
        virtualShardList.add(virtualShardD2);

        virtualShardShardMap.put(virtualShardD0, shardD);
        virtualShardShardMap.put(virtualShardD1, shardD);
        virtualShardShardMap.put(virtualShardD2, shardD);


        rangeMaxValueToVirtualShardMap.put(166, virtualShardD0);
        rangeMaxValueToVirtualShardMap.put(277, virtualShardD1);
        rangeMaxValueToVirtualShardMap.put(388, virtualShardD2);


        int numberOfMovementsNeeded = consistentHash.ReBalance(shardD);

        System.out.println("number of movement needed = "+ numberOfMovementsNeeded);
        System.out.println(consistentHash.getRecordById("1").getValue());
        System.out.println(consistentHash.getRecordById("160").getValue());
        System.out.println(consistentHash.getRecordById("161").getValue());
        System.out.println(consistentHash.getRecordById("162").getValue());
    }
}
