package me.index;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

public final class Utils {
    public static long readLittleEndianLong(DataInputStream dis, boolean isLong) throws IOException {
        long result = 0;
        for (int i = 0; i < (isLong ? 8 : 4); i++) {
            result |= ((long) dis.readUnsignedByte()) << (8 * i);
        }
        return result;
    }

    public static List<Long> read(String filename, int data_size, boolean isLong, boolean shift, boolean plusOne) {
        try {
            DataInputStream stream = new DataInputStream(new BufferedInputStream(new FileInputStream(filename)));
            long fileSize = readLittleEndianLong(stream, true);
            System.out.println("[DEBUG] read size: " + fileSize);

            int size = (int) fileSize;

            List<Long> list = new ArrayList<>(size);
            for (int i = 0; i < size; ++i) {
                long x = readLittleEndianLong(stream, isLong);
                if (list.isEmpty() || list.getLast() != x)
                    list.add(x);
                if (list.size() >= data_size)
                    break;
            }
            System.out.println("[DEBUG] read keys: " + list.size() + " " + list.subList(0, 5) + " "
                    + list.subList(list.size() - 5, list.size()));

            if (shift) {
                list.replaceAll(aLong -> aLong + Long.MIN_VALUE + (plusOne ? 1L : 0L));
            }

            System.out.println("[DEBUG] total keys: " + list.size() + " " + list.subList(0, 5) + " "
                    + list.subList(list.size() - 5, list.size()));

            return list;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static List<Long> generateUniformKeys(int size, boolean isLong, Random rnd) {
        List<Long> lst = new ArrayList<>(size);
        HashSet<Long> st = new HashSet<>();
        for (int i = 0; i < size; i++) {
            while (true) {
                long x = (isLong ? rnd.nextLong() : rnd.nextInt());
                if (!st.contains(x)) {
                    st.add(x);
                    lst.add(x);
                    break;
                }
            }
        }
        Collections.sort(lst);
        System.out.println("[DEBUG] size_uniform: " + lst.size() + " " + lst.subList(0, 5) + " "
                + lst.subList(lst.size() - 5, lst.size()));
        return lst;
    }

    public static List<Long> generateLinearKeys(int size, boolean isLong, Random rnd) {
        int count = rnd.nextInt(size / 1000);
        List<Integer> splines = genSplines(size, count, rnd);
        System.out.println("[DEBUG] size_splines: " + splines.size() + " " + splines.subList(0, 5) + " " +
                splines.subList(splines.size() - 5, splines.size()));
        int pos = 0;

        long maxstep;
        if (isLong) {
            maxstep = (long) 9e18 / (long) size;
        } else {
            maxstep = (long) 2e9 / (long) size;
        }

        List<Long> keys = new ArrayList<>(size);
        keys.add((isLong) ? Long.MIN_VALUE + 1 : Integer.MIN_VALUE + 1);
        long curr = maxstep + rnd.nextLong(maxstep);
        for (int i = 1; i < size; i++) {
            keys.add(keys.get(i - 1) + curr);
            if (pos < splines.size() && i == splines.get(pos)) {
                pos++;
                curr = maxstep + rnd.nextLong(maxstep);
            }
        }

        for (int i = 1; i < keys.size(); i++) {
            if (keys.get(i) <= keys.get(i - 1)) {
                throw new RuntimeException("unsorted keys while linear generate");
            }
        }

        System.out.println("[DEBUG] size_linear: " + keys.size() + " " + keys.subList(0, 5) + " "
                + keys.subList(keys.size() - 5, keys.size()));
        return keys;
    }

    public static List<Integer> genSplines(int n, int count, Random rnd) {
        if (count < 0 || count > n - 2 || n < 2) {
            throw new RuntimeException("error while gensplines");
        }
        List<Integer> perm = new ArrayList<>(n - 2);
        for (int i = 0; i < n - 2; i++) {
            perm.add(i + 1);
        }
        Collections.shuffle(perm, rnd);
        List<Integer> ans = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            ans.add(perm.get(i));
        }
        Collections.sort(ans);
        return ans;
    }

    public static List<Integer> genPerm(int size, Random rnd) {
        List<Integer> list = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            list.add(i);
        }
        Collections.shuffle(list, rnd);
        System.out.println("[DEBUG] size_perm: " + list.size() + " " + list.subList(0, 5) + " "
                + list.subList(list.size() - 5, list.size()));
        return list;
    }

    public static int zipf(int n, Random rnd) {
        if (n <= 0) throw new IllegalArgumentException();
        double s = 1.0;
        for (int it = 0; it < 1000; it++) {
            double u = rnd.nextDouble();
            double x;
            x = Math.exp(u * Math.log(n + 1));
            int k = (int) Math.ceil(x);
            if (k < 1 || k > n) continue;
            double accept = Math.pow(k / x, s);
            if (rnd.nextDouble() < accept) {
                return k - 1;
            }
        }
        return 0;
    }

    public static int x_y(double d, int size, Random rnd) {
        if (rnd.nextDouble() < d) {
            return rnd.nextInt(Math.max(1, (int) (size * (1.0 - d))));
        } else {
            return Math.max(0, size - 1 - rnd.nextInt(Math.max(1, (int) (size * d))));
        }
    }

    public static List<Long> thin(List<Long> k, Random rnd) {
        List<Long> k2 = new ArrayList<>(k.size() / 3);
        for (Long a : k) {
            if (rnd.nextDouble() < 0.3333)
                k2.add(a);
        }
        System.out.println("[DEBUG] thin_size: " + k2.size());
        return k2;
    }
}
