package me.index.config;

public enum WorkloadPerm {
    _true(true), _false(false);

    public final boolean value;

    WorkloadPerm(boolean value) {
        this.value = value;
    }
}
