package me.index.algo;

@FunctionalInterface
public interface TConsumer<T, U, V> {
    void accept(T t, U u, V v);
}
