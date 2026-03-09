package me.index.map;

import me.index.Holder;
import me.index.algo.LRM;
import me.index.algo.Regression;
import me.index.algo.TConsumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class LIndex implements Storage {
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
    private Models models = new Models(List.of(Long.MIN_VALUE), List.of(Long.MAX_VALUE),
            List.of(new Model(List.of(), List.of(new StorageHandler()), new LRM(0, 0, 16), 16)));

    @Override
    public void init(List<Long> keys, List<Object> values, int maxErr) {
        List<Long> min_keys = new ArrayList<>();
        List<Long> max_keys = new ArrayList<>();
        List<Model> models = new ArrayList<>();

        TConsumer<Integer, Integer, LRM> lambda = (start, end, lrm) -> {
            min_keys.add((start != 0 ? keys.get(start) : Long.MIN_VALUE));
            max_keys.add((end < keys.size() ? (keys.get(end) - 1) : Long.MAX_VALUE));
            models.add(new Model(keys.subList(start, end),
                    IntStream.range(start, end)
                    .mapToObj(i -> new StorageHandler(keys.get(i), values.get(i)))
                    .collect(Collectors.toList()),
                    lrm, maxErr));
            models.getLast().bins.add(new StorageHandler());
        };

        Regression regression = new Regression();

        regression.split(keys, maxErr, lambda);

        if(models.isEmpty()){
            min_keys.add(Long.MIN_VALUE);
            max_keys.add(Long.MAX_VALUE);
            models.add(new Model(new ArrayList<>(), new ArrayList<>(), new LRM(0, 0, maxErr), maxErr));
            models.getLast().bins.add(new StorageHandler());
        }
        this.models = new Models(min_keys, max_keys, models);
        size = keys.size();
    }

    private Model getModel(long key){
        int modelIdx = Collections.binarySearch(models.max_keys, key);
        if(modelIdx < 0) modelIdx = -modelIdx - 1;
        return models.models.get(modelIdx);
    }

    private StorageHandler getStorageHandlerM(Model model, long key){
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
                    return model.bins.get(l);
                }
            }
        }
        // If we are here, but no bin has key bigger than searched, give surplus bin.
        return model.bins().getLast();
    }

    private StorageHandler getStorageHandler(long key){
        return getStorageHandlerM(getModel(key), key);
    }


    @Override
    public int find(long key, Holder<Object> result) {
        return getStorageHandler(key).find(key, result);
    }

    @Override
    public int insert(long key, Object value) {
        int verdict = getStorageHandler(key).insert(key, value);
        if(verdict == OK) size++;
        return verdict;
    }

    @Override
    public int remove(long key) {
        int verdict = getStorageHandler(key).remove(key);
        if(verdict == OK) size--;
        return verdict;
    }

    @Override
    public void resort(List<Long> keys, List<Object> vals) {
        for(Model model : models.models){
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

    private record Models(List<Long> min_keys, List<Long> max_keys, List<Model> models) {
    }

    private record Model(List<Long> keys, List<StorageHandler> bins, LRM lrm, int maxErr) {
    }

    private class StorageHandler implements Storage{

        public StorageHandler(){
            state = State.Bin;
            storage = new Bin();
        }

        public StorageHandler(long key, Object value) {
            state = State.Single;
            storage = new Single(key, value);
        }

        private enum State{
            Single, Bin, Lindex
        }
        private State state;
        private Storage storage;

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

        @Override
        public void init(List<Long> keys, List<Object> values, int maxErr) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int find(long key, Holder<Object> result) {
            return storage.find(key, result);
        }

        @Override
        public int insert(long key, Object value) {
            int verdict = storage.insert(key, value);
            if(verdict == REBUILD){
                Storage newStorage;
                if(state == State.Single){
                    state = State.Bin;
                    newStorage = new Bin();
                }else{
                    state = State.Lindex;
                    newStorage = new LIndex();
                }
                List<Long> keys = new ArrayList<>();
                List<Object> vals = new ArrayList<>();
                storage.resort(keys, vals);
                newStorage.init(keys, vals, models.models.getFirst().maxErr);
                storage = newStorage;
                return storage.insert(key, value);
            }
            return verdict;
        }

        @Override
        public int remove(long key) {
            return storage.remove(key);
        }

        @Override
        public void resort(List<Long> keys, List<Object> vals) {
            storage.resort(keys, vals);
        }

        @Override
        public int size() {
            return storage.size();
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
                        LIndex.insert(values, lb, value);
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
                            LIndex.insert(reciever.values, 0, donor.values[donor.n]);
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
                            LIndex.insert(children, lb + 1, rightHalf);
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
