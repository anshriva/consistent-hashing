package com.anubhav.sharding.database.internal;

import java.util.Objects;

public class Record {
    private String key;
    private String value;
    private String virtualShardKey;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getVirtualShardKey() {
        return virtualShardKey;
    }

    public void setVirtualShardKey(String virtualShardKey) {
        this.virtualShardKey = virtualShardKey;
    }

    public Record(String key, String value, String virtualShardKey) {
        this.key = key;
        this.value = value;
        this.virtualShardKey = virtualShardKey;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Record record = (Record) o;
        return Objects.equals(key, record.key) && Objects.equals(value, record.value) && Objects.equals(virtualShardKey, record.virtualShardKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value, virtualShardKey);
    }
}
