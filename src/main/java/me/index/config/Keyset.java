package me.index.config;

public enum Keyset {
    _uniform_int32(false, null),
    _uniform_int64(true, null),
    _p_linear_int32(false, null),
    _p_linear_int64(true, null),
    _wiki_ts_200M_uint64(true, new boolean[]{false, false}),
    _books_200M_uint32(false, new boolean[]{false, false}),
    _books_800M_uint64(true, new boolean[]{true, true}),
    _osm_cellids_800M_uint64(true, new boolean[]{true, false}),
    _fb_200M_uint64(true, new boolean[]{true, false});

    public final boolean isLong;
    public final boolean isSOSD;
    public final boolean isUniform;
    public final boolean isLinear;

    public final boolean needShift;
    public final boolean needPlusOne;

    Keyset(boolean isLong, boolean[] flags) {
        this.isLong = isLong;
        this.isSOSD = (flags != null);
        this.isUniform = (name().equals("_uniform_int32") || name().equals("_uniform_int64"));
        this.isLinear = (name().equals("_p_linear_int32") || name().equals("_p_linear_int64"));
        if (isSOSD) {
            this.needShift = flags[0];
            this.needPlusOne = flags[1];
        } else {
            this.needShift = false;
            this.needPlusOne = false;
        }
    }
}
