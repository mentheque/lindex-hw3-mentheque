package me.index.map;

import me.index.Holder;

import java.util.*;

public class BTreeAdapt implements Storage {
    public static final int t = 32;
    public static final int maxKeys = 2 * t - 1;
    public static float d0 = 0.5f;

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

    private static void insertBoolean(boolean[] a, int idx, boolean value){
        System.arraycopy(a, idx, a, idx + 1, a.length - idx - 1);
        a[idx] = value;
    }


    private static <T> void insert(T[] a, int idx, T value){
        System.arraycopy(a, idx, a, idx + 1, a.length - idx - 1);
        a[idx] = value;
    }

    private static Node take(Node node, int from, int to){
        Node out = new Node(node.leaf);
        out.n = to - from;
        System.arraycopy(node.keys, from, out.keys, 0, (to - from));
        System.arraycopy(node.vals, from, out.vals, 0, (to - from));
        System.arraycopy(node.cnt, from, out.cnt, 0, (to - from));
        System.arraycopy(node.deleted, from, out.deleted, 0, (to - from));
        System.arraycopy(node.childs, from, out.childs, 0, (to - from) + 1);

        out.C = 0;
        out.Mstar = 0;
        /*
        * Невозможно рассчитать их адекватно, если поддерживать только cnt для ключей в вершине.
        * Поэтому считаем что split ~ rebuild, в том плане что он делаем C -> Mstar, C = 0. Этой проблемы было бы
        * меньше для B+-дерева, но как будто бы мы не это хотим? В любом случае, пишу не это.
        * */
        for(int i = 0;i < out.n;i++){
            out.Mstar += out.cnt[i];
        }
        if(!out.leaf){
            for(int i = 0;i <= out.n;i++){
                out.Mstar += out.childs[i].C + out.childs[i].Mstar;
            }
        }

        return out;
    }

    private static Node leftHalf(Node node){
        return take(node, 0, t - 1);
    }
    private static Node rightHalf(Node node){
        return take(node, t, maxKeys);
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
        Node rebuildRoot = null;
        int verdict = FAIL;

        Node current = root;
        while (true) {
            if(current == null){
                verdict = FAIL;
                break;
            }

            current.C++;
            if(current.needsRebuild() && rebuildRoot == null){
                rebuildRoot = current;
            }

            int lb = lowerBound(current.keys, current.n, key);
            if (current.has(key, lb)) {
                if(!current.deleted[lb]){
                    result.v = current.vals[lb];
                    verdict = OK;
                }else{
                    verdict = FAIL;
                }
                current.cnt[lb]++;
                break;
            } else {
                current = current.childs[lb];
            }
        }

        if(rebuildRoot != null){
            rebuildRoot.rebuild();
        }
        return verdict;
    }

    @Override
    public int insert(long key, Object value) {
        Node rebuildRoot = null;
        int verdict = FAIL;

        Node prev = root;
        Node current = root;
        int lb = 0;
        while(true){
            if(current.n == maxKeys){
                if(current == prev){
                    root = new Node(false);
                    prev = root;
                    prev.C = current.C;
                    prev.Mstar = current.Mstar;
                    prev.C++;
                }
                prev.n++;
                insertLong(prev.keys, lb, current.keys[t - 1]);
                insertLong(prev.cnt, lb, current.cnt[t - 1]);
                insertBoolean(prev.deleted, lb, current.deleted[t - 1]);
                insert(prev.vals, lb, current.vals[t - 1]);
                prev.childs[lb] = rightHalf(current);
                insert(prev.childs, lb, leftHalf(current));

                current = prev;
                prev.C--;
            }

            current.C++;
            if(current.needsRebuild() && rebuildRoot == null){
                rebuildRoot = current;
            }


            lb = lowerBound(current.keys, current.n, key);
            if(current.has(key, lb)){
                if(current.deleted[lb]){
                    current.deleted[lb] = false;
                    size++;
                    current.vals[lb] = value;
                    verdict = OK;
                }else{
                    verdict = FAIL;
                }
                break;
            }
            if(current.leaf){
                insertLong(current.keys, lb, key);
                insertLong(current.cnt, lb, 1);
                insert(current.vals, lb, value);
                insertBoolean(current.deleted, lb, false);
                current.n++;
                size++;
                verdict = OK;
                break;
            }else{
                prev = current;
                current = prev.childs[lb];
            }
        }

        if(rebuildRoot != null){
            rebuildRoot.rebuild();
        }
        return verdict;
    }


    @Override
    public int remove(long key) {
        Node rebuildRoot = null;
        int verdict = FAIL;

        Node current = root;
        while (true) {
            if(current == null){
                verdict = FAIL;
                break;
            }

            current.C++;
            if(current.needsRebuild() && rebuildRoot == null){
                rebuildRoot = current;
            }

            int lb = lowerBound(current.keys, current.n, key);
            if (current.has(key, lb)) {
                if(!current.deleted[lb]){
                    current.deleted[lb] = true;
                    size--;
                    verdict = OK;
                }else{
                    verdict = FAIL;
                }
                current.cnt[lb]++;
                break;
            } else {
                current = current.childs[lb];
            }
        }

        if(rebuildRoot != null){
            rebuildRoot.rebuild();
        }
        return verdict;
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
        boolean leaf;
         long[] keys;
         Object[] vals;
         Node[] childs;

        long Mstar;
        long C;
         long[] cnt;
         boolean[] deleted;

        Node(boolean leaf) {
            this.leaf = leaf;
            this.keys = new long[2 * t - 1];
            this.vals = new Object[2 * t - 1];
            this.childs = new Node[2 * t];
            this.cnt = new long[2 * t - 1];
            this.deleted = new boolean[2 * t - 1];
        }

        private void resort(List<Long> keys, List<Object> vals) {
            resort(keys, vals, new ArrayList<>());
        }

        private void resort(List<Long> keys, List<Object> vals, List<Long> cnts) {
            for (int i = 0; i < n; i++) {
                if(!this.leaf) {
                    this.childs[i].resort(keys, vals, cnts);
                }
                if (!this.deleted[i]) {
                    keys.add(this.keys[i]);
                    vals.add(this.vals[i]);
                    cnts.add((cnts.isEmpty() ? 0 : cnts.getLast()) + this.cnt[i]);
                }
            }
            if(!this.leaf) {
                this.childs[n].resort(keys, vals, cnts);
            }
        }

        private boolean has(long key, int pos){
            return (pos >= 0 && pos < n && keys[pos] == key);
        }

        private boolean needsRebuild(){
            return C >= d0 * Mstar;
        }

        private void rebuild() {
            if(!leaf) {
                List<Long> cKeys = new ArrayList<>();
                List<Long> cCnts = new ArrayList<>();
                List<Object> cVals = new ArrayList<>();
                resort(cKeys, cVals, cCnts);
                Node rebuild = build(cKeys, cVals, cCnts, 0);

                n = rebuild.n;
                leaf = rebuild.leaf;
                keys = rebuild.keys;
                vals = rebuild.vals;
                childs = rebuild.childs;

                Mstar = rebuild.Mstar;
                cnt = rebuild.cnt;
                deleted = rebuild.deleted;
            }else{
                Mstar += C;
            }
            C = 0;
        }

        private static long level = 0;

        private static Node build(List<Long> keys, List<Object> vals, List<Long> cnts, long cntAdjust){
            level++;
            Node out = null;
            if(keys.size() <= maxKeys) {
                out = new Node(true);
                for(int i = 0;i < keys.size();i++){
                    out.vals[out.n] = vals.get(i);
                    out.keys[out.n] = keys.get(i);
                    out.cnt[out.n] = (i == 0? cnts.get(i) - cntAdjust: cnts.get(i) - cnts.get(i - 1));
                    out.Mstar += out.cnt[out.n];
                    out.n++;
                }
            }else{
                out = new Node(false);
                out.Mstar = cnts.getLast() - cntAdjust;
                long searchFor;
                int lbound = 0;
                int rbound = 0;
                long lastWeight = cntAdjust;

                for(out.n = 0;out.n < maxKeys && rbound < keys.size() - 1; out.n++){
                    if(out.n != 0){
                        lastWeight = cnts.get(rbound);
                    }

                    searchFor = lastWeight + Math.ceilDiv(cnts.getLast() - lastWeight, maxKeys - out.n);
                    rbound = Collections.binarySearch(cnts, searchFor);
                    if(rbound < 0){
                        rbound = -rbound - 1;
                    }
                    if(rbound < lbound){
                        rbound = lbound;
                    }
                    if(rbound == keys.size()){
                        if(lbound < keys.size()){
                            rbound--;
                        }else{
                            break;
                        }
                    }
                    out.keys[out.n] = keys.get(rbound);
                    out.vals[out.n] = vals.get(rbound);
                    out.cnt[out.n] = cnts.get(rbound) - (rbound == 0? cntAdjust : cnts.get(rbound - 1));
                    out.childs[out.n] = build(keys.subList(lbound, rbound), vals.subList(lbound, rbound),
                            cnts.subList(lbound, rbound), (lbound == 0 ? cntAdjust : cnts.get(lbound - 1)));
                    lbound = rbound + 1;
                }

                if(lbound < keys.size()){
                    rbound = keys.size();
                    out.childs[out.n] = build(keys.subList(lbound, rbound), vals.subList(lbound, rbound),
                            cnts.subList(lbound, rbound), (lbound == 0 ? cntAdjust : cnts.get(lbound - 1)));
                }else{
                    out.childs[out.n] = new Node(true);
                }
            }
            level--;
            return out;
        }
    }

    private Node root;
    private int size;

    public BTreeAdapt() {
        this.root = new Node(true);
    }

}
