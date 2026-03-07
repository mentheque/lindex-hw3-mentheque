package me.index.map;

import me.index.Holder;

import java.util.List;

public interface Storage {
    // verdicts:
    int OK = 1;
    int FAIL = 2;
    int REBUILD = 3;

    // constructor:

    void init(List<Long> keys, List<Object> values, int maxErr);

    // methods:

    int find(long key, Holder<Object> result);

    int insert(long key, Object value);

    int remove(long key);

    void resort(List<Long> keys, List<Object> vals);

    int size();
}
