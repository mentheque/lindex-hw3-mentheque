package me.index.config;

public enum WorkloadFactor {
    _read_only(1.0, 0.0), _real(0.8, 0.1), _write_heavy(0.33, 0.33);

    public final double read;
    public final double insert;

    WorkloadFactor(double read, double insert) {
        this.read = read;
        this.insert = insert;
    }
}
