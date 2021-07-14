package com.yr.net.collect;

/**
 * @author dengbp
 * @ClassName Collector
 * @Description 采集器
 * @date 2020-11-19 18:25
 */
public abstract class Collector {

    private final CollectType type;

    public Collector(CollectType type) {
        this.type = type;
    }

    public CollectType getType() {
        return type;
    }

    /**
     * Description todo
     * @param
     * @return void
     * @throws Exception Exception
     * @Author dengbp
     * @Date 11:14 2020-11-20
     **/

    public abstract void collect();
}
