package me.zhenchuan.files.ext;

import me.zhenchuan.files.Overlord;
import me.zhenchuan.files.Worker;

/**
 * Created by liuzhenchuan@foxmail.com on 1/16/15.
 */
public class EmptyWorker extends Worker{

    public EmptyWorker(String name, Overlord overlord) {
        super(name, overlord);
    }

    @Override
    protected void process(String line) {

    }
}
