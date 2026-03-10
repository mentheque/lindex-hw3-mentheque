package me.index.map;

import me.index.Holder;
import me.index.algo.LRM;
import me.index.algo.Regression;
import me.index.algo.TConsumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

public class LIndexAdapt implements Storage {
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

    private static void insertBoolean(boolean[] a, int idx, boolean value){
        System.arraycopy(a, idx, a, idx + 1, a.length - idx - 1);
        a[idx] = value;
    }


    private int size = 0;

    Node root;
    List<Long> min_keys = new ArrayList<>();
    List<Long> max_keys = new ArrayList<>();
    List<Model> models = new ArrayList<>();
    List<Long> cnts = new ArrayList<>();

    @Override
    public void init(List<Long> keys, List<Object> values, int maxErr) {
        min_keys = new ArrayList<>();
        max_keys = new ArrayList<>();
        models = new ArrayList<>();
        cnts = new ArrayList<>();

        TConsumer<Integer, Integer, LRM> lambda = (start, end, lrm) -> {
            min_keys.add((start != 0 ? keys.get(start) : Long.MIN_VALUE));
            max_keys.add((end < keys.size() ? (keys.get(end) - 1) : Long.MAX_VALUE));
            models.add(new Model(keys.subList(start, end),
                    IntStream.range(start, end)
                            .mapToObj(i -> new Single(keys.get(i), values.get(i)))
                            .collect(Collectors.toList()),
                    lrm, maxErr));
            cnts.add(1L);
            models.getLast().bins.add(new Bin());
        };

        Regression regression = new Regression();

        regression.split(keys, maxErr, lambda);

        if(models.isEmpty()){
            min_keys.add(Long.MIN_VALUE);
            max_keys.add(Long.MAX_VALUE);
            models.add(new Model(new ArrayList<>(), new ArrayList<>(), new LRM(0, 0, maxErr), maxErr));
            models.getLast().bins.add(new Bin());
            cnts.add(0L);
        }
        root = new Node(0, models.size());
        root.build(LongStream.range(0, models.size() + 1).boxed().toList());
        size = keys.size();
    }

    private Model getModel(long key){
        Node current = root;
        int modelIdx;
        int lbmax;
        while(true){
            current.touch();

            for (lbmax = 0; lbmax < current.n; lbmax++)
                if (max_keys.get(current.modelIdxes[lbmax]) >= key) break;

            if(lbmax == current.n || min_keys.get(current.modelIdxes[lbmax]) > key) {
                current = current.children[lbmax];
            }else {
                modelIdx = current.modelIdxes[lbmax];
                cnts.set(modelIdx, cnts.get(modelIdx) + 1);
                return models.get(modelIdx);
            }
        }
    }

    private Storage getStorageM(Model model, long key){
        return model.bins().get(getBinIdxM(model, key));
    }

    private int getBinIdxM(Model model, long key){
        if(!model.keys.isEmpty()) { // Possible when initialising with 0 data
            int pred = Regression.predict(model.lrm.k(), model.lrm.b(), key);
            pred = Math.min(pred, model.keys.size() - 1);
            int l;
            int r;
            if (model.keys.get(pred) >= key) {
                l = Math.max(0, pred - model.maxErr - 1);
                r = pred;
            } else {
                l = pred;
                r = Math.min(model.keys().size() - 1, pred + model.maxErr + 1);
            }
            for (; l <= r; l++) {
                if (model.keys.get(l) >= key) {
                    return l;
                }
            }
        }
        // If we are here, but no bin has key bigger than searched, give surplus bin.
        return model.bins().size() - 1;
    }

    private Storage getStorage(long key){
        return getStorageM(getModel(key), key);
    }


    @Override
    public int find(long key, Holder<Object> result) {
        return getStorage(key).find(key, result);
    }

    @Override
    public int insert(long key, Object value) {
        Model model = getModel(key);
        int binIdx = getBinIdxM(model, key);
        Storage storage = model.bins.get(binIdx);
        int verdict = storage.insert(key, value);
        if(verdict == REBUILD){
            Storage newStorage;
            if(storage.size() <= 1){
                newStorage = new Bin();
            }else{
                newStorage = new LIndex();
            }
            List<Long> keys = new ArrayList<>();
            List<Object> vals = new ArrayList<>();
            storage.resort(keys, vals);
            newStorage.init(keys, vals, models.getFirst().maxErr);

            model.bins.set(binIdx, newStorage);
            verdict = newStorage.insert(key, value);
        }
        if(verdict == OK) size++;
        return verdict;
    }

    @Override
    public int remove(long key) {
        int verdict = getStorage(key).remove(key);
        if(verdict == OK) size--;
        return verdict;
    }

    @Override
    public void resort(List<Long> keys, List<Object> vals) {
        for(Model model : models){
            for(Storage bin : model.bins){
                bin.resort(keys, vals);
            }
        }
    }

    @Override
    public int size() {
        return size;
    }

    // other classes:

    private class Node{
        final private static int t = 32;
        final private static int maxKeys = 2 * t - 1;

        final private static double d0 = 0.25d;

        public int n = 0;
        final int[] modelIdxes;
        final Node[] children;

        private long C = 0;
        private long M = 1;

        private final int modelsFrom;
        private final int modelsTo;

        public Node(int modelsFrom, int modelsTo) {
            modelIdxes = new int[maxKeys];
            children = new Node[maxKeys + 1];
            this.modelsFrom = modelsFrom;
            this.modelsTo = modelsTo;
        }

        public void touch(){
            C++;
            if(C >= d0 * M){
                M += C;
                C = 0;
                if(n != modelsTo - modelsFrom){
                    List<Long> cPref = new ArrayList<>(modelsTo - modelsFrom + 1);
                    cPref.add(0L);
                    for(int modelIdx = modelsFrom;modelIdx < modelsTo;modelIdx++) {
                        cPref.add(cPref.getLast() + cnts.get(modelIdx));
                    }

                    build(cPref);
                }
            }
        }

        public void build(List<Long> cPref){
            M = cPref.getLast() - cPref.getFirst();
            int modelNumber = cPref.size() - 1;
            if(modelNumber <= maxKeys){
                n = modelNumber;
                for(int i =0;i < n;i++){
                    modelIdxes[i] = modelsFrom + i;
                }
            }else{
                n = 0;
                int lbound = 0;
                int rbound = 0;
                while(lbound < modelNumber){
                    long searchFor = cPref.get(lbound) +
                            Math.ceilDiv(cPref.getLast() - cPref.get(lbound),  maxKeys - n);
                    rbound = Collections.binarySearch(cPref.subList(lbound + 1, cPref.size()), searchFor);
                    if(rbound < 0) rbound = -rbound - (rbound < -1? 2 : 1);
                    rbound += lbound;

                    if(rbound > lbound){
                        children[n] = new Node(modelsFrom + lbound, modelsFrom + rbound);
                        children[n].build(cPref.subList(lbound, rbound + 1));
                    }
                    // | Unclear
                    if(rbound != modelNumber){
                        modelIdxes[n] = modelsFrom + rbound;
                        n++;
                    }
                    lbound = rbound + 1;
                }
            }
        }
    }

    private record Model(List<Long> keys, List<Storage> bins, LRM lrm, int maxErr) {
    }

    private static class Single implements Storage{
        long key;
        Object value;
        boolean deleted;

        public Single(long key, Object value) {
            this.key = key;
            this.value = value;
            deleted = false;
        }

        @Override
        public void init(List<Long> keys, List<Object> values, int maxErr) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int find(long key, Holder<Object> result) {
            if(this.key == key && !deleted){
                result.v = value;
                return OK;
            }
            return FAIL;
        }

        @Override
        public int insert(long key, Object value) {
            if(this.key == key){
                if(deleted){
                    deleted = false;
                    this.value = value;
                    return OK;
                }
                return FAIL;
            }
            return REBUILD;
        }

        @Override
        public int remove(long key) {
            if(this.key == key){
                if(deleted){
                    return FAIL;
                }
                deleted = true;
                return OK;
            }
            return FAIL;
        }

        @Override
        public void resort(List<Long> keys, List<Object> vals) {
            if(!deleted){
                keys.add(key);
                vals.add(value);
            }
        }

        @Override
        public int size() {
            return (deleted? 0 : 1);
        }
    }

    public static class Bin implements Storage{
        private final static int parent_size = 8;
        private final static int child_size = 16;

        private BinNode root = null;

        private class BinNode implements Storage{
            final boolean child;

            int n;
            final BinNode[] children;
            final long[] keys;
            final boolean[] deleted;
            final Object[] values;

            public BinNode(boolean child) {
                this.child = child;
                if(child){
                    children = null;
                    keys = new long[child_size];
                    values = new Object[child_size];
                    deleted = new boolean[child_size];
                }else{
                    children = new BinNode[parent_size + 1];
                    keys = new long[parent_size];
                    values = null;
                    deleted = null;
                }
                n = 0;
            }

            @Override
            public void init(List<Long> keys, List<Object> values, int maxErr) {
                throw new UnsupportedOperationException();
            }

            @Override
            public int find(long key, Holder<Object> result) {
                int lb = lowerBound(keys, n, key);
                if(child){
                    if(lb == n || keys[lb] != key || deleted[lb]){
                        return FAIL;
                    }else{
                        result.v = values[lb];
                        return OK;
                    }
                }else{
                    return children[lb].find(key, result);
                }
            }

            // basically just insert, but also updating the parent
            private void addOnTop(int childIdx, long key, Object value, boolean deleted){
                BinNode reciever = children[childIdx];
                reciever.keys[reciever.n] = key;
                reciever.values[reciever.n] = value;
                reciever.deleted[reciever.n] = deleted;
                keys[childIdx] = reciever.keys[reciever.n];
                reciever.n++;
            }

            @Override
            public int insert(long key, Object value) {
                int lb = lowerBound(keys, n, key);
                if(child){
                    if(lb < n && keys[lb] == key){
                        if(deleted[lb]){
                            deleted[lb] = false;
                            values[lb] = value;
                            return OK;
                        }
                        return FAIL;
                    }
                    if(n < child_size){
                        insertLong(keys, lb, key);
                        LIndexAdapt.insert(values, lb, value);
                        insertBoolean(deleted, lb, false);
                        n++;
                        return OK;
                    }else{
                        return REBUILD;
                    }
                }else{
                    int childVerdict = children[lb].insert(key, value);
                    if(childVerdict == REBUILD) {
                        BinNode donor = children[lb];
                        if (lb - 1 >= 0 && children[lb - 1].n < child_size) {
                            // donating lowest to the left
                            if(donor.keys[0] < key){
                                // lb doesn't change after moving lowest value
                                addOnTop(lb - 1, donor.keys[0], donor.values[0], donor.deleted[0]);

                                System.arraycopy(donor.keys, 1, donor.keys, 0, donor.n - 1);
                                System.arraycopy(donor.values, 1, donor.values, 0, donor.n - 1);
                                System.arraycopy(donor.deleted, 1, donor.deleted, 0, donor.n - 1);
                                donor.n--;

                                return donor.insert(key, value);
                            }else{
                                // inserted would be the lowest, so just put it on top of previous bin
                                addOnTop(lb - 1, key, value, false);
                                return OK;
                            }
                        } else if (lb + 1 <= n && children[lb + 1].n < child_size) {
                            // donating highest to the right
                            BinNode reciever = children[lb + 1];
                            donor.n--;
                            insertLong(reciever.keys, 0, donor.keys[donor.n]);
                            LIndexAdapt.insert(reciever.values, 0, donor.values[donor.n]);
                            insertBoolean(reciever.deleted, 0, donor.deleted[donor.n]);
                            reciever.n++;
                            if(donor.keys[donor.n - 1] > key){
                                // lb doesn't change after moving, so properly insert
                                keys[lb] = donor.keys[donor.n - 1];
                                return donor.insert(key, value);
                            }else{
                                // inserted is the biggest, so just add on top
                                addOnTop(lb, key, value, false);
                                return OK;
                            }
                        } else if (n < parent_size) {
                            // split the child
                            int splitPoint = child_size / 2;
                            int rhSize = child_size - splitPoint;
                            BinNode rightHalf = new BinNode(true);
                            System.arraycopy(donor.keys, splitPoint, rightHalf.keys, 0, rhSize);
                            System.arraycopy(donor.values, splitPoint, rightHalf.values, 0, rhSize);
                            System.arraycopy(donor.deleted, splitPoint, rightHalf.deleted, 0, rhSize);
                            donor.n = splitPoint;
                            rightHalf.n = rhSize;
                            if(lb + 1 != parent_size){
                                insertLong(keys, lb + 1, keys[lb]); // rightHalf top key
                            }
                            keys[lb] = donor.keys[donor.n - 1];     // left half top key
                            LIndexAdapt.insert(children, lb + 1, rightHalf);
                            n++;
                            if(keys[lb] < key){
                                lb = lb + 1;
                            }
                            return children[lb].insert(key, value);
                        }
                        return REBUILD;
                    }
                    return childVerdict;
                }
            }

            @Override
            public int remove(long key) {
                int lb = lowerBound(keys, n, key);
                if(child){
                    if(lb == n || keys[lb] != key || deleted[lb]){
                        return FAIL;
                    }else{
                        deleted[lb] = true;
                        return OK;
                    }
                }else{
                    return children[lb].remove(key);
                }
            }

            @Override
            public void resort(List<Long> keys, List<Object> vals) {
                if(child){
                    for(int i = 0;i < n;i++){
                        if(!deleted[i]){
                            keys.add(this.keys[i]);
                            vals.add(this.values[i]);
                        }
                    }
                }else{
                    for(int i = 0;i <= n;i++){
                        children[i].resort(keys, vals);
                    }
                }
            }

            @Override
            public int size() {
                // O(size) because no one actually needs to call this
                int out = 0;
                if(child){
                    for(int i = 0;i < n;i++){
                        if(!deleted[i]){
                            out++;
                        }
                    }
                }else{
                    for(int i = 0;i <= n;i++){
                        out += children[i].size();
                    }
                }
                return out;
            }
        }

        @Override
        public void init(List<Long> keys, List<Object> values, int maxErr) {
            for(int i = 0;i < keys.size();i++){
                insert(keys.get(i), values.get(i));
            }
        }

        @Override
        public int find(long key, Holder<Object> result) {
            if(root == null){
                return FAIL;
            }else {
                return root.find(key, result);
            }
        }

        @Override
        public int insert(long key, Object value) {
            if(root == null){
                root = new BinNode(true);
            }
            int verdict = root.insert(key, value);
            if(verdict == REBUILD){
                if(root.child){
                    BinNode parent = new BinNode(false);
                    parent.n = 1;
                    parent.children[0] = root;
                    parent.keys[0] = root.keys[root.n - 1];
                    parent.children[1] = new BinNode(true);
                    root = parent;
                    return insert(key, value);
                }
            }
            return verdict;
        }

        @Override
        public int remove(long key) {
            if(root == null){
                return FAIL;
            }
            return root.remove(key);
        }

        @Override
        public void resort(List<Long> keys, List<Object> vals) {
            if(root != null){
                root.resort(keys, vals);
            }
        }

        @Override
        public int size() {
            if(root != null){
                return root.size();
            }
            return 0;
        }
    }
}
