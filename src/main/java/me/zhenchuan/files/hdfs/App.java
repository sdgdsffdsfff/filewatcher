package me.zhenchuan.files.hdfs;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.j256.ormlite.dao.Dao;
import com.j256.ormlite.dao.DaoManager;
import com.j256.ormlite.jdbc.JdbcConnectionSource;
import com.j256.ormlite.stmt.QueryBuilder;
import com.j256.ormlite.support.ConnectionSource;
import com.j256.ormlite.table.TableUtils;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static spark.Spark.*;
import spark.*;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by liuzhenchuan@foxmail.com on 1/23/15.
 */
public class App {

    private static final Logger log = LoggerFactory.getLogger(App.class);

    private final static String DATABASE_URL = "jdbc:h2:file:~/db/h2meta.db";

    public static void main(String[] args) throws Exception {
        OptionParser parser = new OptionParser();

        OptionSpec<String> propertyPath = parser.accepts("property").withRequiredArg().required();
        OptionSpec<Integer> minute = parser.accepts("minute" ).withRequiredArg().ofType( Integer.class ).defaultsTo(25);
        OptionSpec<Integer> portOpt = parser.accepts("port" ).withRequiredArg().ofType( Integer.class ).defaultsTo(3456);


        OptionSet options = parser.parse(args);

        final int triggerMinute = minute.value(options);
        final int port = portOpt.value(options);

        setPort(port);



        Properties properties = new Properties();
        properties.load(new FileInputStream(new File(propertyPath.value(options))));

        HdfsFileBatchBuilder builder = new HdfsFileBatchBuilder()
                .setName(properties.getProperty("name"))
                .setHdfsPathPattern(properties.getProperty("hdfs_pattern"))
                .setBaseWorkPath(properties.getProperty("work_path"))
                .setFilenamePattern(properties.getProperty("file_pattern"))
                .setGran(properties.getProperty("gran"))
                .setMaxUploadSize(NumberUtils.toLong(properties.getProperty("max_upload_size"),200 * 1024 * 1024))
                .setSafeInterval(NumberUtils.toInt(properties.getProperty("safe_interval"),10))
                .setTmpDir(properties.getProperty("tmp_dir","/tmp"));

        log.info("config:\n{}" , builder.toString());

        final HdfsFileBatch hdfsFileBatch = builder
                .createHdfsFileBatch();

        final Dao<HdfsMeta, Integer> hdfsDao = initDao();
        hdfsFileBatch.setHdfsDao(hdfsDao);

        final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
        scheduledExecutor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                int minute = new DateTime().getMinuteOfHour();
                if (minute == triggerMinute) {
                    hdfsFileBatch.process();
                }
            }
        }, 0, 60, TimeUnit.SECONDS);

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    log.info("begin to stop...........");
                    hdfsFileBatch.stop();
                    scheduledExecutor.shutdown();
                    scheduledExecutor.awaitTermination(10, TimeUnit.MINUTES);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }));

        get(new Route("history") {
            @Override
            public Object handle(Request request, Response response) {

                String app = request.queryParams("app");
                String limit = request.queryParams("limit");
                try {
                    QueryBuilder<HdfsMeta, Integer> queryBuilder = hdfsDao.queryBuilder();
                    queryBuilder.where().eq("app",app);
                    queryBuilder.orderBy("gran", false).orderBy("startTime", true) ;
                    queryBuilder.limit(NumberUtils.toLong(limit, 500));
                    List<HdfsMeta> metas = queryBuilder.query();
                    return decorate(metas);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return false;
            }

            private String decorate(List<HdfsMeta> metas) {
                ImmutableListMultimap<String,HdfsMeta> multimap = Multimaps.index(metas,new Function<HdfsMeta, String>() {
                    @Nullable
                    @Override
                    public String apply(@Nullable HdfsMeta hdfsMeta) {
                        return hdfsMeta.getGran();
                    }
                });

                StringBuilder sb = new StringBuilder("<table border='1' cellspacing='0'>");
                sb.append("<tr><td>app</td><td>gran</td>" +
                        "<td>duration(ms)</td>" +
                        "<td>file num</td><td>file size</td><td>info</td>" +
                        "<td>start time</td><td>end time</td></tr>") ;

                for(String key : multimap.keySet()){
                    long sumFileSize = 0 ;
                    long sumFileNum = 0 ;
                    for(HdfsMeta meta : multimap.get(key)){
                        sumFileSize += meta.getFileSize();
                        sumFileNum += meta.getFileNum();

                        sb.append("<tr>");
                        sb.append("<td>").append(meta.getApp()).append("</td>");
                        sb.append("<td>").append(meta.getGran()).append("</td>");

                        sb.append("<td>").append(meta.getEndTime() - meta.getStartTime()).append("</td>");
                        sb.append("<td>").append(meta.getFileNum()).append("</td>");
                        sb.append("<td>").append(meta.getFileSize()).append("</td>");
                        sb.append("<td>").append(meta.getInfo()).append("</td>");
                        sb.append("<td>").append(DateFormatUtils.format(meta.getStartTime(),"yyyy-MM-dd HH:mm:ss")).append("</td>");
                        sb.append("<td>").append(DateFormatUtils.format(meta.getEndTime(),"yyyy-MM-dd HH:mm:ss")).append("</td>");
                        sb.append("</tr>");
                    }
                    sb.append("<tr><td colspan='3'></td><td>").append(sumFileNum).append("</td><td>").append(sumFileSize).append("</td><td colspan='3'></td></tr>");
                }
                sb.append("</table>");
                return sb.toString();
            }
        });

    }

    public static Dao<HdfsMeta, Integer> initDao(){
        try {
            ConnectionSource connectionSource = new JdbcConnectionSource(DATABASE_URL);
            TableUtils.createTableIfNotExists(connectionSource, HdfsMeta.class);
            return DaoManager.createDao(connectionSource, HdfsMeta.class);
        }catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

}
