package me.zhenchuan.files.hdfs;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Properties;

/**
 * Created by liuzhenchuan@foxmail.com on 1/23/15.
 */
public class App {

    private static final Logger log = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) throws Exception {
        OptionParser parser = new OptionParser();

        OptionSpec<String> propertyPath = parser.accepts("property").withRequiredArg().required();
        OptionSet options = parser.parse(args);

        Properties properties = new Properties();
        properties.load(new FileInputStream(new File(propertyPath.value(options))));

        HdfsFileBatchBuilder builder = new HdfsFileBatchBuilder()
                .setHdfsPathPattern(properties.getProperty("hdfs_pattern"))
                .setBaseWorkPath(properties.getProperty("work_path"))
                .setFilenamePattern(properties.getProperty("file_pattern"))
                .setGran(properties.getProperty("gran"))
                .setMaxUploadSize(NumberUtils.toInt(properties.getProperty("max_upload_size"),200 * 1024 * 1024))
                .setTmpDir(properties.getProperty("tmp_dir","/tmp"));

        log.info("config:\n{}" , builder.toString());

        final HdfsFileBatch hdfsFileBatch = builder
                .createHdfsFileBatch();
        hdfsFileBatch.process();

    }

}
