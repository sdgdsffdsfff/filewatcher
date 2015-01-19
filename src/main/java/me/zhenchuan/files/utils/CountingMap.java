package me.zhenchuan.files.utils;


import com.google.common.collect.*;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by liuzhenchuan@foxmail.com on 1/16/15.
 */
public class CountingMap<K extends Comparable> extends ConcurrentHashMap<K, AtomicLong>{

    public void incr(K key){
        AtomicLong retVal = super.get(key);
        if (retVal == null) {
            retVal = super.putIfAbsent((K) key, new AtomicLong(1)) ;
        }
        if(retVal != null){
            retVal.getAndIncrement();
        }
    }

    /****
     * 按key排序
     * @return
     */
    public Map<K, Long> snapshot(){
        ImmutableSortedMap.Builder<K, Long> builder = new ImmutableSortedMap.Builder<K, Long>(Ordering.<K>natural());
        for (Map.Entry<K, AtomicLong> entry : entrySet()) {
            builder.put(entry.getKey(), entry.getValue().get());
        }
        return builder.build();
    }
}
