package me.zhenchuan.files.ext;

import me.zhenchuan.files.Overlord;
import me.zhenchuan.files.Worker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Created by liuzhenchuan@foxmail.com on 1/15/15.
 */
public class ConsoleWorker extends Worker {

    private static final Logger log = LoggerFactory.getLogger(ConsoleWorker.class);

    public ConsoleWorker(String name, Overlord overlord) {
        super(name, overlord);
    }

    @Override
    public void init() {
        Properties properties = super.getOverlord().getProperties();
        System.out.println("init using " + properties);
    }

    @Override
    protected void process(String line) {
        System.out.println(line);
    }


}
