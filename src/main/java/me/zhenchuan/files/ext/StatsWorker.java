package me.zhenchuan.files.ext;

import me.zhenchuan.files.Overlord;
import me.zhenchuan.files.Worker;
import me.zhenchuan.files.utils.CountingMap;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by liuzhenchuan@foxmail.com on 1/16/15.
 */
public class StatsWorker extends Worker{

    private static CountingMap<String> countingMap  = new CountingMap<>();
    private static AtomicLong counter = new AtomicLong(0);
    private static long startTime = System.currentTimeMillis() ;

    private String outputFile ;

    public StatsWorker(String name, Overlord overlord) {
        super(name, overlord);
    }

    @Override
    protected void init() {
        super.init();
        Properties properties = super.getOverlord().getProperties();
        if(properties!=null){
            this.outputFile = properties.getProperty("stats.output.path", "/tmp/stats.log");
        }
    }

    @Override
    protected void process(String line) {
        if (StringUtils.isEmpty(line)) {
            return ;
        }
        String[] items = StringUtils.splitByWholeSeparatorPreserveAllTokens(
                line, "\t");
        String platform = items[3]; //平台
        String date = items[6].substring(0,10); //请求时间(yyyyMMddHHmmssSSS）
        countingMap.incr(platform.concat(":".concat(date)));
        if(counter.incrementAndGet() % 500000 == 0){
            try {
                System.out.println(counter.get() + "\t" + (System.currentTimeMillis() -startTime ) + "ms\t" + countingMap.snapshot().toString());
                FileUtils.writeStringToFile(new File(outputFile), countingMap.snapshot().toString());
                startTime = System.currentTimeMillis();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
