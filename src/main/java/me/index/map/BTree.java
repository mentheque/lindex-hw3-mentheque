package me.index.map;

import me.index.Holder;

import java.util.List;

import static java.lang.System.arraycopy;

public class BTree implements Storage {
    public static final int t = 32;
    public static final int maxKeys = 2 * t - 1;

    public static int lowerBound(long[] a, int n, long key) {
        int i;
        for (i = 0; i < n; i++)
            if (a[i] >= key) return i;
        return i;
    }

    public static final class Node {
        int n;
        final boolean leaf;
        final long[] keys;
        final Object[] vals;
        final Node[] child;

        Node(boolean leaf) {
            this.leaf = leaf;
            this.keys = new long[2 * t - 1];
            this.vals = new Object[2 * t - 1];
            this.child = new Node[2 * t];
        }

        void resort(List<Long> k, List<Object> v) {
            if (leaf) {
                for (int i = 0; i < n; i++) {
                    k.add(keys[i]);
                    v.add(vals[i]);
                }
            } else {
                for (int i = 0; i < n; i++) {
                    child[i].resort(k, v);
                    k.add(keys[i]);
                    v.add(vals[i]);
                }
                child[n].resort(k, v);
            }
        }
    }

    private Node root;
    private int size;

    public BTree() {
        this.root = new Node(true);
        this.size = 0;
    }

    @Override
    public void init(List<Long> keys, List<Object> values, int maxErr) {
        for (int i = 0; i < keys.size(); i++)
            this.insert(keys.get(i), values.get(i));
    }

    @Override
    public int find(long key, Holder<Object> result) {
        Node x = root;
        while (true) {
            int i = lowerBound(x.keys, x.n, key);
            if (i < x.n && x.keys[i] == key) {
                result.v = x.vals[i];
                return OK;
            }
            if (x.leaf) {
                return FAIL;
            }
            x = x.child[i];
        }
    }

    @Override
    public int insert(long key, Object value) {
        if (root.n == maxKeys) {
            Node x = new Node(root.leaf);
            arraycopy(root.keys, t, x.keys, 0, t - 1);
            arraycopy(root.vals, t, x.vals, 0, t - 1);
            if (!root.leaf) arraycopy(root.child, t, x.child, 0, t);
            x.n = t - 1;
            root.n = t - 1;
            Node r = new Node(false);
            r.child[0] = root;
            r.child[1] = x;
            r.keys[0] = root.keys[t - 1];
            r.vals[0] = root.vals[t - 1];
            r.n++;
            root = r;
        }
        Node x = root;
        while (true) {
            int i = lowerBound(x.keys, x.n, key);
            if (i < x.n && x.keys[i] == key) return FAIL;
            if (x.leaf) {
                if (x.n - i > 0) {
                    arraycopy(x.keys, i, x.keys, i + 1, x.n - i);
                    arraycopy(x.vals, i, x.vals, i + 1, x.n - i);
                }
                x.keys[i] = key;
                x.vals[i] = value;
                x.n++;
                size++;
                return OK;
            }
            Node c = x.child[i];
            if (c.n == maxKeys) {
                Node y = x.child[i];
                Node z = new Node(y.leaf);
                arraycopy(y.keys, t, z.keys, 0, t - 1);
                arraycopy(y.vals, t, z.vals, 0, t - 1);
                if (!y.leaf) arraycopy(y.child, t, z.child, 0, t);
                z.n = t - 1;
                y.n = t - 1;
                arraycopy(x.child, i + 1, x.child, i + 2, x.n - i);
                arraycopy(x.keys, i, x.keys, i + 1, x.n - i);
                arraycopy(x.vals, i, x.vals, i + 1, x.n - i);
                x.child[i + 1] = z;
                x.keys[i] = y.keys[t - 1];
                x.vals[i] = y.vals[t - 1];
                x.n++;
                if (x.keys[i] == key) return FAIL;
                if (key > x.keys[i]) i++;
            }
            x = x.child[i];
        }
    }

    @Override
    public int remove(long key) {
        Node x = root;
        long k = key;
        while (true) {
            int i = lowerBound(x.keys, x.n, k);
            if (i < x.n && x.keys[i] == k) {
                if (x.leaf) {
                    if ((x.n - i - 1) > 0) {
                        arraycopy(x.keys, i + 1, x.keys, i, x.n - i - 1);
                        arraycopy(x.vals, i + 1, x.vals, i, x.n - i - 1);
                    }
                    x.n--;
                    x.vals[x.n] = null;
                    if (root.n == 0 && !root.leaf) root = root.child[0];
                    size--;
                    return OK;
                } else {
                    long q = x.keys[i];
                    Node l = x.child[i];
                    Node r = x.child[i + 1];
                    if (l.n >= t) {
                        Node c = l;
                        while (!c.leaf) c = c.child[c.n];
                        x.keys[i] = c.keys[c.n - 1];
                        x.vals[i] = c.vals[c.n - 1];
                        x = l;
                        k = c.keys[c.n - 1];
                    } else if (r.n >= t) {
                        Node c = r;
                        while (!c.leaf) c = c.child[0];
                        x.keys[i] = c.keys[0];
                        x.vals[i] = c.vals[0];
                        x = r;
                        k = c.keys[0];
                    } else {
                        Node u = x.child[i];
                        Node w = x.child[i + 1];
                        u.keys[t - 1] = x.keys[i];
                        u.vals[t - 1] = x.vals[i];
                        arraycopy(w.keys, 0, u.keys, t, w.n);
                        arraycopy(w.vals, 0, u.vals, t, w.n);
                        if (!u.leaf) arraycopy(w.child, 0, u.child, t, w.n + 1);
                        u.n += w.n + 1;
                        arraycopy(x.keys, i + 1, x.keys, i, x.n - i - 1);
                        arraycopy(x.vals, i + 1, x.vals, i, x.n - i - 1);
                        x.vals[x.n - 1] = null;
                        arraycopy(x.child, i + 2, x.child, i + 1, x.n - i - 1);
                        x.child[x.n] = null;
                        x.n--;
                        x = l;
                        k = q;
                    }
                }
            } else {
                if (x.leaf) return FAIL;
                boolean flag = (i == x.n);
                if (x.child[i].n < t) {
                    if (i > 0 && x.child[i - 1].n >= t) {
                        Node u = x.child[i];
                        Node w = x.child[i - 1];
                        arraycopy(u.keys, 0, u.keys, 1, u.n);
                        arraycopy(u.vals, 0, u.vals, 1, u.n);
                        if (!u.leaf) arraycopy(u.child, 0, u.child, 1, u.n + 1);
                        u.keys[0] = x.keys[i - 1];
                        u.vals[0] = x.vals[i - 1];
                        if (!u.leaf) {
                            u.child[0] = w.child[w.n];
                            w.child[w.n] = null;
                        }
                        x.keys[i - 1] = w.keys[w.n - 1];
                        x.vals[i - 1] = w.vals[w.n - 1];
                        w.vals[w.n - 1] = null;
                        w.n--;
                        u.n++;
                    } else if (i < x.n && x.child[i + 1].n >= t) {
                        Node u = x.child[i];
                        Node w = x.child[i + 1];
                        u.keys[u.n] = x.keys[i];
                        u.vals[u.n] = x.vals[i];
                        if (!u.leaf) {
                            u.child[u.n + 1] = w.child[0];
                            arraycopy(w.child, 1, w.child, 0, w.n);
                            w.child[w.n] = null;
                        }
                        x.keys[i] = w.keys[0];
                        x.vals[i] = w.vals[0];
                        arraycopy(w.keys, 1, w.keys, 0, w.n - 1);
                        arraycopy(w.vals, 1, w.vals, 0, w.n - 1);
                        w.n--;
                        w.vals[w.n] = null;
                        u.n++;
                    } else {
                        Node u;
                        Node w;
                        if (i < x.n) {
                            u = x.child[i];
                            w = x.child[i + 1];
                            u.keys[t - 1] = x.keys[i];
                            u.vals[t - 1] = x.vals[i];
                            arraycopy(w.keys, 0, u.keys, t, w.n);
                            arraycopy(w.vals, 0, u.vals, t, w.n);
                            if (!u.leaf) arraycopy(w.child, 0, u.child, t, w.n + 1);
                            arraycopy(x.keys, i + 1, x.keys, i, x.n - i - 1);
                            arraycopy(x.vals, i + 1, x.vals, i, x.n - i - 1);
                            arraycopy(x.child, i + 2, x.child, i + 1, x.n - i - 1);
                        } else {
                            u = x.child[i - 1];
                            w = x.child[i];
                            u.keys[t - 1] = x.keys[i - 1];
                            u.vals[t - 1] = x.vals[i - 1];
                            arraycopy(w.keys, 0, u.keys, t, w.n);
                            arraycopy(w.vals, 0, u.vals, t, w.n);
                            if (!u.leaf) arraycopy(w.child, 0, u.child, t, w.n + 1);
                            arraycopy(x.keys, i, x.keys, i - 1, x.n - i);
                            arraycopy(x.vals, i, x.vals, i - 1, x.n - i);
                            arraycopy(x.child, i + 1, x.child, i, x.n - i);
                        }
                        u.n += w.n + 1;
                        x.vals[x.n - 1] = null;
                        x.child[x.n] = null;
                        x.n--;
                    }
                }
                if (flag && i > x.n) i--;
                x = x.child[i];
            }
        }
    }

    @Override
    public void resort(List<Long> keys, List<Object> vals) {
        root.resort(keys, vals);
    }

    @Override
    public int size() {
        return size;
    }
}
