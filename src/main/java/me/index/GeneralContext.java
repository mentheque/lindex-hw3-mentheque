package me.index;

import me.index.config.*;

import java.io.FileInputStream;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public final class GeneralContext {
    public final Config cfg;

    public final List<Long> keys;
    public final List<Long> thin_keys;
    public final List<Integer> perm;
    public long[] queries;
    public int[] queryTypes;

    public final int ksize;
    public final int qcount = (int) 1e8;

    public GeneralContext(Properties props, String sosd_path) {
        Random rnd = new Random(42);

        cfg = Config.read(props);
        if (!cfg.keyset().isLong && !cfg.keyset().isSOSD && cfg.dataSize().name().equals("_max")) {
            throw new RuntimeException("incorrect properties: max size for integer keys is not available");
        }

        System.out.println("[DEBUG] " + cfg);

        // --------------------------------------------------
        System.out.println("[DEBUG] start generate keys");

        if (cfg.keyset().isSOSD) {
            keys = Utils.read(sosd_path + (cfg.keyset().name()).substring(1),
                    cfg.dataSize().size, cfg.keyset().isLong, cfg.keyset().needShift, cfg.keyset().needPlusOne);
        } else if (cfg.keyset().isUniform) {
            keys = Utils.generateUniformKeys(cfg.dataSize().size, cfg.keyset().isLong, rnd);
        } else if (cfg.keyset().isLinear) {
            keys = Utils.generateLinearKeys(cfg.dataSize().size, cfg.keyset().isLong, rnd);
        } else {
            throw new RuntimeException("unexpected case");
        }

        ksize = keys.size();

        // --------------------------------------------------
        System.out.println("[DEBUG] start generate thin keys");

        thin_keys = Utils.thin(keys, rnd);

        // --------------------------------------------------
        System.out.println("[DEBUG] start generate perm");

        perm = Utils.genPerm(ksize, rnd);

        // --------------------------------------------------
        System.out.println("[DEBUG] start generate queries");

        queries = new long[qcount];
        for (int i = 0; i < qcount; i++) {
            if (cfg.workload() == Workload._zipf) {
                queries[i] = (cfg.workloadPerm().value)
                        ? keys.get(perm.get(Utils.zipf(ksize, rnd)))
                        : keys.get(Utils.zipf(ksize, rnd));
            } else if (cfg.workload() == Workload._uniform) {
                queries[i] = (cfg.workloadPerm().value)
                        ? keys.get(perm.get(rnd.nextInt(ksize)))
                        : keys.get(rnd.nextInt(ksize));
            } else if (cfg.workload() == Workload._x_y_9999) {
                queries[i] = (cfg.workloadPerm().value)
                        ? keys.get(perm.get(Utils.x_y(0.9999, ksize, rnd)))
                        : keys.get(Utils.x_y(0.9999, ksize, rnd));
            } else if (cfg.workload() == Workload._x_y_99) {
                queries[i] = (cfg.workloadPerm().value)
                        ? keys.get(perm.get(Utils.x_y(0.99, ksize, rnd)))
                        : keys.get(Utils.x_y(0.99, ksize, rnd));
            } else if (cfg.workload() == Workload._x_y_90) {
                queries[i] = (cfg.workloadPerm().value)
                        ? keys.get(perm.get(Utils.x_y(0.90, ksize, rnd)))
                        : keys.get(Utils.x_y(0.90, ksize, rnd));
            } else {
                throw new RuntimeException("unexpected case");
            }
        }

        // 0 - read, 1 - insert, 2 - remove
        queryTypes = new int[qcount];
        for (int i = 0; i < qcount; i++) {
            double d = rnd.nextDouble();
            queryTypes[i] = (d < cfg.workloadFactor().read) ? 0 : (
                    (d < (cfg.workloadFactor().read + cfg.workloadFactor().insert)) ? 1 : 2
            );
        }
    }

    public static GeneralContext read(String filename, String sosd_path) {
        try (FileInputStream fis = new FileInputStream(filename)) {
            Properties props = new Properties();
            props.load(fis);
            return new GeneralContext(props, sosd_path);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
