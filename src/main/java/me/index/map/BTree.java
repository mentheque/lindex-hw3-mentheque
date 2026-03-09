package me.index.map;

import me.index.Holder;

import java.util.List;

public class BTree implements Storage {
    public static final int t = 32;
    public static final int maxKeys = 2 * t - 1;

    private static int lowerBound(long[] a, int n, long key) {
        int i;
        for (i = 0; i < n; i++)
            if (a[i] >= key) return i;
        return i;
    }

    private static void insertLong(long[] a, int idx, long key){
        System.arraycopy(a, idx, a, idx + 1, a.length - idx - 1);
        a[idx] = key;
    }

    private static <T> void insert(T[] a, int idx, T value){
        System.arraycopy(a, idx, a, idx + 1, a.length - idx - 1);
        a[idx] = value;
    }

    private static void eraseLong(long[] a, int idx){
        System.arraycopy(a, idx + 1, a, idx, a.length - idx - 1);
    }

    private static <T> void erase(T[] a, int idx){
        System.arraycopy(a, idx + 1, a, idx, a.length - idx - 1);
    }

    private static Node take(Node node, int from, int to){
        Node out = new Node(node.leaf);
        out.n = to - from;
        System.arraycopy(node.keys, from, out.keys, 0, (to - from));
        System.arraycopy(node.vals, from, out.vals, 0, (to - from));
        System.arraycopy(node.childs, from, out.childs, 0, (to - from) + 1);
        return out;
    }

    private static Node leftHalf(Node node){
        return take(node, 0, t - 1);
    }
    private static Node rightHalf(Node node){
        return take(node, t, maxKeys);
    }

    private static Node mergeFillMiddle(Node lhs, Node rhs, long fillKey, Object fillVal){
        System.arraycopy(rhs.keys, 0, lhs.keys, lhs.n + 1, rhs.n);
        System.arraycopy(rhs.vals, 0, lhs.vals, lhs.n + 1, rhs.n);
        System.arraycopy(rhs.childs, 0, lhs.childs, lhs.n + 1, rhs.n + 1);
        lhs.keys[lhs.n] = fillKey;
        lhs.vals[lhs.n] = fillVal;
        lhs.n += rhs.n + 1;
        return lhs;
    }


    @Override
    public void init(List<Long> keys, List<Object> values, int maxErr) {
        size = 0;
        root = new Node(true);
        for(int i = 0;i < keys.size();i++){
            insert(keys.get(i), values.get(i));
        }
    }

    @Override
    public int find(long key, Holder<Object> result) {
        Node current = root;
        while (true) {
            int lb = lowerBound(current.keys, current.n, key);
            if (current.has(key, lb)) {
                result.v = current.vals[lb];
                return OK;
            } else if (current.leaf) {
                return FAIL;
            } else {
                current = current.childs[lb];
            }
        }
    }

    @Override
    public int insert(long key, Object value) {
        Node current = root;
        Node prev = root;
        int lb = 0;
        while(true){
            if(current.n == maxKeys){
                if(current == prev){
                    root = new Node(false);
                    prev = root;
                }
                prev.n++;
                insertLong(prev.keys, lb, current.keys[t - 1]);
                insert(prev.vals, lb, current.keys[t - 1]);
                prev.childs[lb] = rightHalf(current);
                insert(prev.childs, lb, leftHalf(current));
                current = prev;
            }
            lb = lowerBound(current.keys, current.n, key);
            if(current.has(key, lb)){
                current.vals[lb] = value;
                return FAIL;
            }
            if(current.leaf){
                insertLong(current.keys, lb, key);
                insert(current.vals, lb, value);
                current.n++;
                size++;
                return OK;
            }else{
                prev = current;
                current = prev.childs[lb];
            }
        }
    }


    @Override
    public int remove(long key) {
        Node intermid = null;

        Node current = root;
        Node prev = root;
        int lb = 0;
        while(true){
            if(current != root && current.n == t - 1){
                if(lb != 0 && prev.childs[lb - 1].n >= t){
                    //small right rotation
                    Node donor = prev.childs[lb - 1];
                    insertLong(current.keys, 0, prev.keys[lb - 1]);
                    insert(current.vals, 0, prev.vals[lb - 1]);
                    insert(current.childs, 0, donor.childs[donor.n]);
                    donor.n--;
                    current.n++;
                    // donor n is already decreased, so no need for -1:
                    prev.keys[lb - 1] = donor.keys[donor.n];
                    prev.vals[lb - 1] = donor.vals[donor.n];
                }else if(lb != prev.n && prev.childs[lb + 1].n >= t){
                    // small left rotation
                    Node donor = prev.childs[lb + 1];
                    current.keys[current.n] = prev.keys[lb];
                    current.vals[current.n] = prev.vals[lb];
                    current.childs[current.n + 1] = donor.childs[0];
                    prev.keys[lb] = donor.keys[0];
                    prev.vals[lb] = donor.vals[0];

                    eraseLong(donor.keys, 0);
                    erase(donor.vals, 0);
                    erase(donor.childs, 0);
                    donor.n--;
                    current.n++;
                }else {
                    if(lb != 0) {
                        // merge left
                        current = prev.childs[lb - 1]
                                = mergeFillMiddle(prev.childs[lb - 1], current, prev.keys[lb - 1], prev.vals[lb - 1]);
                        eraseLong(prev.keys, lb - 1);
                        erase(prev.vals, lb - 1);
                        erase(prev.childs, lb);
                    }else if(lb != prev.n) {
                        // merge right
                        mergeFillMiddle(current, prev.childs[lb + 1], prev.keys[lb], prev.vals[lb]);
                        eraseLong(prev.keys, lb);
                        erase(prev.vals, lb);
                        erase(prev.childs, lb + 1);
                    }
                    prev.n--;
                    if (prev == root && root.n == 0) {
                        root = current;
                    }
                }

            }
            lb = lowerBound(current.keys, current.n, key);
            if(current.leaf){
                if(current.has(key, lb)) {
                    eraseLong(current.keys, lb);
                    erase(current.vals, lb);
                    current.n--;
                    size--;
                    return OK;
                } else if(intermid != null){
                    lb = lowerBound(intermid.keys, intermid.n, key);
                    if(!intermid.has(key, lb)){
                        throw new AssertionError("Lost removing key somewhere");
                    }
                    intermid.keys[lb] = current.keys[current.n - 1];
                    intermid.vals[lb] = current.vals[current.n - 1];
                    current.n--;
                    size--;
                    return OK;
                }
                return FAIL;
            }
            if(current.has(key, lb)) {
                intermid = current;
            }

            prev = current;
            current = prev.childs[lb];
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

    private static final class Node {
        int n;
        final boolean leaf;
        final long[] keys;
        final Object[] vals;
        final Node[] childs;

        Node(boolean leaf) {
            this.leaf = leaf;
            this.keys = new long[2 * t - 1];
            this.vals = new Object[2 * t - 1];
            this.childs = new Node[2 * t];
        }

        private void resort(List<Long> keys, List<Object> vals) {
            if(leaf){
                for(int i = 0;i < n;i++){
                    keys.add(this.keys[i]);
                    vals.add(this.vals[i]);
                }
            }else{
                for(int i = 0;i < n;i++){
                    this.childs[i].resort(keys, vals);
                    keys.add(this.keys[i]);
                    vals.add(this.vals[i]);
                }
                this.childs[n].resort(keys, vals);
            }
        }

        private boolean has(long key, int pos){
            return (pos >= 0 && pos < n && keys[pos] == key);
        }

    }

    private Node root;
    private int size;

    public BTree() {
        this.root = new Node(true);
    }

}
