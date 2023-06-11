package com.anubhav.sharding.hashing;

import com.anubhav.sharding.database.internal.Record;
import com.anubhav.sharding.database.internal.Shard;
import com.anubhav.sharding.database.internal.VirtualShard;
import com.anubhav.sharding.database.DaoService;

import java.util.List;
import java.util.Optional;

public class ConsistentHash implements IConsistentHash
{
    private DaoService daoService;
    private HashingHelper hashingHelper;

    public ConsistentHash(DaoService daoService, HashingHelper hashingHelper) {
        this.daoService = daoService;
        this.hashingHelper = hashingHelper;
    }

    @Override
    public void insert(RecordDo recordDo) {
        String id = recordDo.getKey();
        int hash = this.hashingHelper.hash(id);

        int[] ranges = this.daoService.getRangesAssignedInAscendingOrder();

        int serverRangeEndingPoint = this.hashingHelper.getServerEndingRangeFromGivenHash(hash, ranges);

        Shard shard = this.daoService.getShardFromEndRange(serverRangeEndingPoint);
        VirtualShard virtualShard = this.daoService.getVirtualShardFromEndRange(serverRangeEndingPoint);

        Record record = new Record(recordDo.getKey(), recordDo.getValue(), virtualShard.getVirtualShardKey());
        this.daoService.addRecordToShard(shard.getShardId(), record);
    }

    @Override
    public RecordDo getRecordById(String id) {
        int hash = this.hashingHelper.hash(id);

        int[] ranges = this.daoService.getRangesAssignedInAscendingOrder();

        int serverRangeEndingPoint = this.hashingHelper.getServerEndingRangeFromGivenHash(hash, ranges);

        Shard shard = this.daoService.getShardFromEndRange(serverRangeEndingPoint);
        VirtualShard virtualShard = this.daoService.getVirtualShardFromEndRange(serverRangeEndingPoint);

        List<Record> recordList = this.daoService.getRecordsBySharedIdAndVirtualShardId(shard.getShardId(), virtualShard.getVirtualShardKey());

        Optional<Record> recordOptional = recordList.stream().filter(record -> record.getKey().equalsIgnoreCase(id)).findFirst();

        if(recordOptional.isPresent()){
            return  new RecordDo(recordOptional.get().getKey(), recordOptional.get().getValue());
        }
        return null;
    }

    @Override
    public int ReBalance(Shard shardAdded) {
        int[] ranges = this.daoService.getRangesAssignedInAscendingOrder();
        int numberOfReBalance = 0;
        for(int i =0 ;i < ranges.length -1 ; i++){
            var shardForVirtualShardInGivenRange = this.daoService.getShardFromEndRange(ranges[i]);
            if(shardForVirtualShardInGivenRange.equals(shardAdded)){
                VirtualShard nextServer = this.daoService.getVirtualShardFromEndRange(ranges[i+1]);
                Shard shardNextServer = this.daoService.getShardFromEndRange(ranges[i+1]);

                numberOfReBalance += this.reBalanceVirtualServerAndGetCount(shardNextServer, nextServer);
            }
        }
        return numberOfReBalance;
    }

    private  int reBalanceVirtualServerAndGetCount(Shard shard, VirtualShard virtualShard){
        List<Record> records = this.daoService.getRecordsBySharedIdAndVirtualShardId(shard.getShardId(), virtualShard.getVirtualShardKey());
        int rebalanced = 0;
        for(int i =0;i< records.size();i++){
            String key = records.get(i).getKey();
            int hash = this.hashingHelper.hash(key);
            int[] ranges = this.daoService.getRangesAssignedInAscendingOrder();
            int serverRangeEndingPoint = this.hashingHelper.getServerEndingRangeFromGivenHash(hash, ranges);
            Shard newShard = this.daoService.getShardFromEndRange(serverRangeEndingPoint);
            VirtualShard newVirtualShard = this.daoService.getVirtualShardFromEndRange(serverRangeEndingPoint);
            // If shard id is changed, then just remove the data from current shard and
            // assign to the new shard
            if(!newShard.getShardId().equalsIgnoreCase(shard.getShardId())){
                rebalanced ++;
                this.daoService.removeRecordFromShard(shard.getShardId(), key);
                Record record = new Record(key, records.get(i).getValue(),newVirtualShard.getVirtualShardKey());
                this.daoService.addRecordToShard(newShard.getShardId(), record);
            }
            // if shard id is same and only virtual id is changed,
            // then no need to rebalance, just change the virtual shard mapping
            else if (!newVirtualShard.getVirtualShardKey().equalsIgnoreCase(virtualShard.getVirtualShardKey())){
                records.get(i).setVirtualShardKey(newVirtualShard.getVirtualShardKey());
            }
        }
        System.out.println("number of re-balances done in "
                + shard.getShardId() +
                " for " +
                 virtualShard.getVirtualShardKey() + " = " +
                rebalanced);
        return rebalanced;
    }

}
