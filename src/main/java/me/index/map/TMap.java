package me.index.map;

import me.index.Holder;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class TMap implements Storage {
    private final TreeMap<Long, Object> root;

    public TMap() {
        this.root = new TreeMap<>();
    }

    @Override
    public void init(List<Long> keys, List<Object> values, int maxErr) {
        for (int i = 0; i < keys.size(); i++)
            this.insert(keys.get(i), values.get(i));
    }

    @Override
    public int find(long key, Holder<Object> result) {
        Object r = root.get(key);
        if (r == null) {
            return FAIL;
        } else {
            result.v = r;
            return OK;
        }
    }

    @Override
    public int insert(long key, Object value) {
        Object r = root.putIfAbsent(key, value);
        if (r == null) {
            return OK;
        } else {
            return FAIL;
        }
    }

    @Override
    public int remove(long key) {
        Object r = root.remove(key);
        if (r == null) {
            return FAIL;
        } else {
            return OK;
        }
    }

    @Override
    public void resort(List<Long> keys, List<Object> vals) {
        for (Map.Entry<Long, Object> e : root.entrySet()) {
            keys.add(e.getKey());
            vals.add(e.getValue());
        }
    }

    @Override
    public int size() {
        return root.size();
    }
}
