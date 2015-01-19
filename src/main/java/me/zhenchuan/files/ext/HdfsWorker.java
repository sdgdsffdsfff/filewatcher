package me.zhenchuan.files.ext;

import me.zhenchuan.files.Overlord;
import me.zhenchuan.files.Worker;

/**
 * Created by liuzhenchuan@foxmail.com on 1/15/15.
 */
public class HdfsWorker extends Worker{

    public HdfsWorker(String name, Overlord overlord) {
        super(name, overlord);
    }

    @Override
    protected void init() {
        super.init();
    }

    @Override
    protected void process(String line) {

    }
}
