package com.anubhav.sharding.hashing;

import com.anubhav.sharding.database.internal.Shard;

public interface IConsistentHash {
    void insert(RecordDo recordDo);
    RecordDo getRecordById(String id);
    int ReBalance(Shard shardAdded);
}
