package me.index.algo;

import java.util.List;

public interface Splittable {
    void split(List<Long> keys, int maxErr, TConsumer<Integer, Integer, LRM> lambda);
}
