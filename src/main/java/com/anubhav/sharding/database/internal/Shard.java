package com.anubhav.sharding.database.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Shard {
    private List<Record> records;

    private String shardId;

    public Shard(String shardId) {
        this.shardId = shardId;
        this.records = new ArrayList<>();
    }

    public String getShardId() {
        return this.shardId;
    }
    public List<Record> getRecords() {
        return this.records;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Shard shard = (Shard) o;
        return Objects.equals(records, shard.records) && Objects.equals(shardId, shard.shardId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(records, shardId);
    }
}
