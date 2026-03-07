package me.index.algo;

import java.util.List;

public class Regression implements Splittable {
    // ----- store the current segment: -----
    RadixSpline segment;
    // --------------------------------------

    public Regression() {
    }

    @Override
    public void split(List<Long> keys, int maxErr, TConsumer<Integer, Integer, LRM> lambda) {
        int start = 0;
        while (start < keys.size()) {
            int i = 0;
            double[] cf = new double[]{0.0, 0.0};
            // ----- init: -----
            segment = new RadixSpline(keys.get(start + i), maxErr);
            i++;
            // -----------------
            while (start + i < keys.size() && can_expand(keys.get(start + i), i, maxErr, cf)) {
                i++;
            }
            // ----- updating weights: -----
            if (segment.done) {
                cf[0] = (segment.y) / (segment.x);
                cf[1] = -cf[0] * segment.key0;
            }
            // -----------------------------
            int err = 0;
            for (int j = start; j < start + i; j++)
                err = Math.max(err, Math.abs(predict(cf[0], cf[1], keys.get(j)) - (j - start)));
            LRM lrm = new LRM(cf[0], cf[1], err);
            if (lrm.maxErr() > 1000) {
                System.out.println("[WARNING] large error due to the limited precision of doubles in LRM, "
                        + "found: " + lrm.maxErr() + ", expected: " + maxErr);
            }
            lambda.accept(start, start + i, lrm);
            start += i;
        }
    }

    public boolean can_expand(long key, int pos, int maxErr, double[] cf) {
        return segment.add(key, pos); // corrected
    }

    public static int predict(double k, double b, long x) {
        int pos = (int) (k * x + b);
        return Math.max(pos, 0);
    }

    // ----- main: -----
    public static class RadixSpline {
        double key0;
        long eps;
        boolean done;

        double x;
        double y;
        double ux;
        double uy;
        double lx;
        double ly;

        public RadixSpline(double key0, long eps) {
            this.key0 = key0;
            this.eps = eps;
            this.done = false;
        }

        public boolean add(double key, double pos) {
            if (!done) {
                done = true;
                x = (key - key0);
                y = pos;
                ux = x;
                uy = y + eps;
                lx = x;
                ly = y - eps;
                return true;
            }
            if (cross((key - key0), pos, ux, uy) > 0 &&
                    cross((key - key0), pos, lx, ly) < 0) {
                x = (key - key0);
                y = pos;
                if (cross(ux, uy, x, y + eps) < 0) {
                    ux = x;
                    uy = y + eps;
                }
                if (cross(lx, ly, x, y - eps) > 0) {
                    lx = x;
                    ly = y - eps;
                }
                return true;
            }
            return false;
        }

        public static double cross(double ax, double ay, double bx, double by) {
            return ax * by - bx * ay;
        }
    }
    // -----------------
}
