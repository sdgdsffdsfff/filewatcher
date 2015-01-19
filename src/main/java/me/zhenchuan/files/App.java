package me.zhenchuan.files;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by liuzhenchuan@foxmail.com on 1/14/15.
 */
public class App {


    public static void main(String[] args) {
        OptionParser parser = new OptionParser();
        OptionSpec<Integer> parallelism = parser.accepts("parallelism" ).withRequiredArg().ofType( Integer.class ).defaultsTo(4);
        OptionSpec<Integer> frequency = parser.accepts("frequency" ).withRequiredArg().ofType( Integer.class ).defaultsTo(1);
        OptionSpec<String> metaPath = parser.accepts("meta_path").withRequiredArg().required();
        OptionSpec<String> workPath = parser.accepts("work_path").withRequiredArg().required();
        OptionSpec<String> propertyPath = parser.accepts("property").withRequiredArg();
        OptionSpec<String> worker = parser.accepts("worker").withRequiredArg().required();
        OptionSpec<String> acceptModifyTime = parser.accepts("acceptModifyTime").withRequiredArg();
        OptionSpec<String> fileNamePattern = parser.accepts("file_pattern").withRequiredArg().required();
        OptionSpec<String> name = parser.accepts("name").withRequiredArg().required();
        OptionSet options = parser.parse(args);

        final Overlord overlord = new OverlordBuilder()
                .setName(name.value(options))
                .setWorker(worker.value(options))
                .setBaseMetaPath(metaPath.value(options))
                .setBaseWorkPath(workPath.value(options))
                .setFileNamePattern(fileNamePattern.value(options))
                .setAcceptModifyTime(acceptModifyTime.value(options))
                .setParallelism(parallelism.value(options))
                .setProperties(propertyPath.value(options))
                .setCheckpointFrequencyInSecond(frequency.value(options))
                .createOverlord();

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                overlord.stop();
            }
        }));

        overlord.dispatch();
    }


}
