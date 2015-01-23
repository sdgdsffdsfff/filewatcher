package me.zhenchuan.files.hdfs;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import me.zhenchuan.files.utils.Files;
import me.zhenchuan.files.utils.Granularity;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.util.*;

/**
 * Created by liuzhenchuan@foxmail.com on 1/22/15.
 */
public class HdfsFileBatch {

    private static final Logger log = LoggerFactory.getLogger(HdfsFileBatch.class);

    private Granularity granularity ;

    private String baseWorkPath ;
    private String tmpDir ;
    private String filenamePattern ;
    private int maxUploadSize;
    private String hdfsPathPattern ;

    public HdfsFileBatch(String baseWorkPath,String tmpDir,String filenamePattern,String hdfsPathPattern,
                         String gran ,int maxUploadSize){
        this.baseWorkPath = baseWorkPath ;
        this.tmpDir = tmpDir;
        this.filenamePattern = filenamePattern ;
        this.granularity = Granularity.valueOf(gran.toUpperCase());
        this.maxUploadSize = maxUploadSize ;
        this.hdfsPathPattern = hdfsPathPattern;
        assertDir();
    }

    public void assertDir(){
        if(!new File(this.tmpDir).exists()){
            new File(this.tmpDir).mkdirs();
        }
        if(!new File(this.baseWorkPath).exists()){
            throw new IllegalArgumentException("work dir ["+this.baseWorkPath+"] not exist!");
        }
    }

    public void process(){
        long s = System.currentTimeMillis() ;
        DateTime lastGran = granularity.prev(new DateTime()); //上一个时间
        String pattern = DateTimeFormat.forPattern(filenamePattern).print(lastGran);
        String remoteFilePath = DateTimeFormat.forPattern(hdfsPathPattern).print(lastGran);

        List<File> files = new ArrayList<File>(FileUtils.listFiles(new File(baseWorkPath)   //根据filepattern找到上一小时的日志文件
                , new RegexFileFilter(pattern)
                , TrueFileFilter.INSTANCE));

        Collections.sort(files,new Comparator<File>() {    //根据文件修改日期排序,
            @Override
            public int compare(File o1, File o2) {
                return Long.valueOf(o1.lastModified()).compareTo(o2.lastModified());
            }
        });

        List<File> container = new ArrayList<>();
        List<String> filePaths = new ArrayList<>();  //记录当前已同步的文件,便于进行对比,是否有新增文件.

        int batchSize = 0 ;
        int totalSize = 0 ;
        for(File file : files){
            container.add(file);
            batchSize += file.length();

            totalSize += file.length();

            if(batchSize >= this.maxUploadSize) {   //大小进行切分(200M)
                upload(remoteFilePath, container);  //上传到hdfs
                batchSize = 0 ;
                container.clear();
            }

            filePaths.add(file.getAbsolutePath());
        }

        upload(remoteFilePath,container);

        //TODO 监测上上N小时的文件是否发生变化,如果有变化,则同步增量数据并酌情通知监控url.

        log.info("upload using {}ms. file num {} .file size {} ",(System.currentTimeMillis() -s ),filePaths.size() ,totalSize );

    }

    private void upload(String remoteFilePath, List<File> container) {
        if(container == null || container.size() ==0 ) return  ;

        List<String> filePathList = Lists.transform(container, new Function<File, String>() {
            @Nullable
            @Override
            public String apply(@Nullable File file) {
                return file.getAbsolutePath();
            }
        });

        File firstFile = container.get(0),lastFile = container.get(container.size() -1);

        File output = Files.concat(
                outputFile(firstFile.getName() + "_" +  lastFile.getName()),
                container
        );

        if(output.exists()){
            boolean flag = Files.upload(
                    (remoteFilePath.endsWith("/")?remoteFilePath  : remoteFilePath + "/" ) + output.getName() ,
                    output.getAbsolutePath());
            if(flag){
                log.info("upload {} to path {} success.\n{}", output.getAbsoluteFile(), remoteFilePath,
                        filePathList);
                output.delete();
            }else{
                log.warn("upload {} to path {} failed.\n{}", output.getAbsoluteFile(), remoteFilePath,filePathList);
            }
        }else{
            log.info("failed to concat files {} \n{}",filePathList);
        }
    }

    private File outputFile(String name){
        if(name == null)
            name = String.valueOf(System.currentTimeMillis()) ;
        return new File(this.tmpDir , name);
    }

    public static void main(String[] args) {
        DateTime dateTime = Granularity.HOUR.prev(new DateTime());
        DateTimeFormat.forPattern("'/user/zhenchuan.liu/tmp/logs/'yyyy/MM/dd/HH").print(dateTime);
        DateTimeFormat.forPattern("yyyyMMddHH'.*.unbid.log'").print(dateTime);
    }

}
