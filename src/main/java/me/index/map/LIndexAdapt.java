package me.index.map;

import me.index.Holder;
import me.index.algo.LRM;
import me.index.algo.Regression;
import me.index.algo.TConsumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class LIndexAdapt implements Storage {
    private final TreeMap<Long, Object> root;

    public LIndexAdapt() {
        this.root = new TreeMap<>();
    }

    @Override
    public void init(List<Long> keys, List<Object> values, int maxErr) {
        List<Long> min_keys = new ArrayList<>();
        List<Long> max_keys = new ArrayList<>();
        List<Model> models = new ArrayList<>();

        TConsumer<Integer, Integer, LRM> lambda = (start, end, lrm) -> {
            min_keys.add((start != 0 ? keys.get(start) : Long.MIN_VALUE));
            max_keys.add((end < keys.size() ? (keys.get(end) - 1) : Long.MAX_VALUE));
            models.add(new Model(keys.subList(start, end), values.subList(start, end), lrm, maxErr));
        };

        Regression regression = new Regression();

        regression.split(keys, maxErr, lambda);

        new Models(min_keys, max_keys, models);

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

    // other classes:

    private record Models(List<Long> min_keys, List<Long> max_keys, List<Model> models) {
    }

    private record Model(List<Long> keys, List<Object> values, LRM lrm, int maxErr) {
    }
}
