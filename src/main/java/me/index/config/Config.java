package me.index.config;

import java.util.Properties;

public record Config(
        Keyset keyset,
        Workload workload,
        WorkloadPerm workloadPerm,
        WorkloadFactor workloadFactor,
        DataSize dataSize,
        MaxErr maxErr
) {
    public static Config read(Properties p) {
        try {
            Keyset k = Keyset.valueOf(p.getProperty("keyset"));
            Workload w = Workload.valueOf(p.getProperty("workload.distribution"));
            WorkloadPerm wp = WorkloadPerm.valueOf(p.getProperty("workload.permutation"));
            WorkloadFactor wf = WorkloadFactor.valueOf(p.getProperty("workload.factor"));
            DataSize ds = DataSize.valueOf(p.getProperty("data.size"));
            MaxErr err = MaxErr.valueOf(p.getProperty("max.err"));
            return new Config(k, w, wp, wf, ds, err);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
