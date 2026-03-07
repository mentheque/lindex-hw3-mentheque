package me.index;

import me.index.algo.Regression;
import me.index.algo.Splittable;
import me.index.map.*;

import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestMain {
    public final int SEED = 237;
    public final int SIZE = (int) 1e7;

    public int split_test_common(Splittable splittable, List<Long> keys, int maxErr) {
        Holder<Integer> last = new Holder<>(0);
        Holder<Integer> cnt = new Holder<>(0);
        splittable.split(keys, maxErr, (start, end, lrm) -> {
            cnt.v = cnt.v + 1;
            assertEquals(last.v, start, "(start == 0)");
            last.v = end;
            int err = 0;
            for (int i = start; i < end; i++) {
                int p_pos = Math.max(0, (int) (lrm.k() * keys.get(i) + lrm.b()));
                int a_pos = i - start;
                err = Math.max(err, Math.abs(p_pos - a_pos));
            }
            assertTrue(err <= lrm.maxErr(), String.format("(err %d <= lrm.maxErr() %d)", err, lrm.maxErr()));
            if (err > maxErr) {
                System.out.printf("[WARNING] (err %d > maxErr %d)%n", err, maxErr);
            }
        });
        assertEquals(last.v, keys.size(), "(end == keys.size())");
        return cnt.v;
    }

    // ---------- split_test weak: ----------

    @org.junit.jupiter.api.Test
    public void split_test_weak_linear() {
        List<Long> L_KEYS_32 = Utils.generateLinearKeys(SIZE, false, new Random(SEED));
        for (int maxErr = 0; maxErr <= 128; maxErr = (maxErr == 0) ? (maxErr + 1) : (maxErr * 2)) {
            System.out.println("[DEBUG] split_test weak linear32, err = " + maxErr + ", segments: " +
                    split_test_common(new Regression(), L_KEYS_32, maxErr));
        }
    }

    @org.junit.jupiter.api.Test
    public void split_test_weak_uniform() {
        List<Long> U_KEYS_32 = Utils.generateUniformKeys(SIZE, false, new Random(SEED));
        for (int maxErr = 0; maxErr <= 128; maxErr = (maxErr == 0) ? (maxErr + 1) : (maxErr * 2)) {
            System.out.println("[DEBUG] split_test weak uniform32, err = " + maxErr + ", segments: " +
                    split_test_common(new Regression(), U_KEYS_32, maxErr));
        }
    }

    // ----- split_test strict: -----

    @org.junit.jupiter.api.Test
    public void split_test_strict_linear() {
        List<Long> L_KEYS_64 = Utils.generateLinearKeys(SIZE, true, new Random(SEED));
        for (int maxErr = 0; maxErr <= 128; maxErr = (maxErr == 0) ? (maxErr + 1) : (maxErr * 2)) {
            System.out.println("[DEBUG] split_test strict linear64, err = " + maxErr + ", segments: " +
                    split_test_common(new Regression(), L_KEYS_64, maxErr));
        }
    }

    @org.junit.jupiter.api.Test
    public void split_test_strict_uniform() {
        List<Long> U_KEYS_64 = Utils.generateUniformKeys(SIZE, true, new Random(SEED));
        for (int maxErr = 0; maxErr <= 128; maxErr = (maxErr == 0) ? (maxErr + 1) : (maxErr * 2)) {
            System.out.println("[DEBUG] split_test strict uniform64, err = " + maxErr + ", segments: " +
                    split_test_common(new Regression(), U_KEYS_64, maxErr));
        }
    }

    // --------------------------------------------------

    public final int max_err_stress = 2;
    public final int query_cnt_stress = 30000;
    public final long min_val_stress = -5000;
    public final long max_val_stress = 5000;
    public final long[] init_keys_stress = new long[]{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048};

    public void stress_test_storage(Storage user, Storage jury, Writer out) {
        Random rnd = new Random(SEED);
        for (int i = 0; i < query_cnt_stress; i++) {
            long x = rnd.nextLong(min_val_stress, max_val_stress + 1);
            int t = rnd.nextInt(3);
            if (t == 0) {
                int r_user = user.insert(x, x);
                int r_jury = jury.insert(x, x);
                assertEquals(r_jury, r_user);
                assertTrue(check(jury, user, out));
            } else if (t == 1) {
                int r_user = user.remove(x);
                int r_jury = jury.remove(x);
                assertEquals(r_jury, r_user);
                assertTrue(check(jury, user, out));
            } else {
                Holder<Object> ans_user = new Holder<>(Long.MIN_VALUE);
                Holder<Object> ans_jury = new Holder<>(Long.MIN_VALUE);
                int r_user = user.find(x, ans_user);
                int r_jury = jury.find(x, ans_jury);
                assertEquals(r_jury, r_user);
                assertEquals(ans_jury.v, ans_user.v);
                assertTrue(check(jury, user, out));
            }
        }
        assertTrue(check(jury, user, out));
    }

    public static boolean check(Storage t, Storage o, Writer out) {
        if (t.size() != o.size())
            return false;

        List<Long> k1 = new ArrayList<>(t.size());
        List<Object> v1 = new ArrayList<>(t.size());
        t.resort(k1, v1);

        List<Long> k2 = new ArrayList<>(o.size());
        List<Object> v2 = new ArrayList<>(o.size());
        o.resort(k2, v2);

        assert k1.size() == k2.size();
        assert k1.size() == v1.size();
        assert k2.size() == v2.size();

        for (int i = 0; i < k1.size(); i++) {
            if (!k1.get(i).equals(k2.get(i)))
                return false;
            if (!v1.get(i).equals(v2.get(i)))
                return false;
        }

        try {
            if (out != null) {
                out.write("check: " + k1.size() + " " + k2.size());
                out.write("\n");
                out.write(k1.toString());
                out.write("\n");
                out.write(k2.toString());
                out.write("\n");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return true;
    }

    @org.junit.jupiter.api.Test
    public void stress_test_btree() {
        System.out.println("[DEBUG] stress_test_btree");
        Storage user = new BTree();
        user.init(new ArrayList<>(init_keys_stress.length),
                new ArrayList<>(init_keys_stress.length),
                max_err_stress);
        Storage jury = new TMap();
        jury.init(new ArrayList<>(init_keys_stress.length),
                new ArrayList<>(init_keys_stress.length),
                max_err_stress);
        stress_test_storage(user, jury, null);
    }

    @org.junit.jupiter.api.Test
    public void stress_test_btree_adapt() {
        System.out.println("[DEBUG] stress_test_btree_adapt");
        Storage user = new BTreeAdapt();
        user.init(new ArrayList<>(init_keys_stress.length),
                new ArrayList<>(init_keys_stress.length),
                max_err_stress);
        Storage jury = new TMap();
        jury.init(new ArrayList<>(init_keys_stress.length),
                new ArrayList<>(init_keys_stress.length),
                max_err_stress);
        stress_test_storage(user, jury, null);
    }

    @org.junit.jupiter.api.Test
    public void stress_test_l_index() {
        System.out.println("[DEBUG] stress_test_l_index");
        Storage user = new LIndex();
        user.init(new ArrayList<>(init_keys_stress.length),
                new ArrayList<>(init_keys_stress.length),
                max_err_stress);
        Storage jury = new TMap();
        jury.init(new ArrayList<>(init_keys_stress.length),
                new ArrayList<>(init_keys_stress.length),
                max_err_stress);
        stress_test_storage(user, jury, null);
    }

    @org.junit.jupiter.api.Test
    public void stress_test_l_index_adapt() {
        System.out.println("[DEBUG] stress_test_l_index_adapt");
        Storage user = new LIndexAdapt();
        user.init(new ArrayList<>(init_keys_stress.length),
                new ArrayList<>(init_keys_stress.length),
                max_err_stress);
        Storage jury = new TMap();
        jury.init(new ArrayList<>(init_keys_stress.length),
                new ArrayList<>(init_keys_stress.length),
                max_err_stress);
        stress_test_storage(user, jury, null);
    }

}
