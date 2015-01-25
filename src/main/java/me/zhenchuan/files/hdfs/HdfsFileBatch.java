package me.zhenchuan.files.hdfs;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.j256.ormlite.dao.Dao;
import com.j256.ormlite.stmt.DeleteBuilder;
import me.zhenchuan.files.utils.Files;
import me.zhenchuan.files.utils.Granularity;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
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
    private long maxUploadSize;
    private String hdfsPathPattern ;

    private String name = "xxx";
    private int safeInterval = 4 ;

    private volatile boolean running = true;

    private Dao<HdfsMeta, Integer> hdfsDao;


    public HdfsFileBatch(String name,String baseWorkPath,String tmpDir,String filenamePattern,String hdfsPathPattern,
                         String gran ,long maxUploadSize,int safeInterval){
        this.name = name ;
        this.baseWorkPath = baseWorkPath ;
        this.tmpDir = tmpDir;
        this.filenamePattern = filenamePattern ;
        this.granularity = Granularity.valueOf(gran.toUpperCase());
        this.maxUploadSize = maxUploadSize ;
        this.hdfsPathPattern = hdfsPathPattern;
        this.safeInterval = safeInterval;
        assertDir();
    }

    public void setHdfsDao(Dao<HdfsMeta, Integer> hdfsDao) {
        this.hdfsDao = hdfsDao;
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
        DateTime dateTime = new DateTime();
        for(int i = 0 ; i < safeInterval ; i++){
            if(!running){
                log.info("the system may be has set to stop ...");
                break;
            }
            DateTime time = dateTime.minus(granularity.getUnits(i));
            process(time);
        }
    }

    public void stop(){
        running = false;
    }

    public void process(DateTime dateTime){
        long startTime = System.currentTimeMillis() ;
        DateTime lastGran = granularity.prev(dateTime); //上一个时间,已经被truncate
        String pattern = DateTimeFormat.forPattern(filenamePattern).print(lastGran);

        String finalRemoteFilePath = DateTimeFormat.forPattern(hdfsPathPattern).print(lastGran);

        List<File> files = findFiles(pattern);

        List<File> batchContainer = new ArrayList<>();

        String yyyyMMddHHmmss = DateTimeFormat.forPattern("yyyyMMddHHmmss").print(lastGran);
        String metaDir = (tmpDir.endsWith("/") ? tmpDir : tmpDir + "/") + name + "/" + yyyyMMddHHmmss;
        if(!new File(metaDir).exists()) {
            try {
                //重置状态
                DeleteBuilder<HdfsMeta, Integer> deleteBuilder = hdfsDao.deleteBuilder();
                deleteBuilder.where().eq("app",name).and().eq("gran",yyyyMMddHHmmss);
                deleteBuilder.delete();
                Files.delete(finalRemoteFilePath);
            } catch (SQLException e) {
                e.printStackTrace();
            }
            new File(metaDir).mkdirs() ;
        }

        List<String> processed = restore(metaDir);

        long batchSize = 0 ;
        long totalSize = 0 ;
        int fileNum = 0;
        for(File file : files){
            if(!running){
                log.info("the system may be has set to stop ...");
                break;
            }
            //判断是否已经处理过.
            if(processed.contains(file.getAbsolutePath())){
                continue;
            }

            batchContainer.add(file);

            batchSize += file.length();
            totalSize += file.length();
            fileNum ++ ;

            if(batchSize >= this.maxUploadSize) {   //大小进行切分(200M)
                uploadAndSaveMeta(finalRemoteFilePath, batchContainer, metaDir);
                batchSize = 0 ;
                batchContainer.clear();
            }
        }

        uploadAndSaveMeta(finalRemoteFilePath, batchContainer, metaDir);


        long endTime = System.currentTimeMillis();
        long timeInMills = endTime - startTime;
        log.info("[{}] [{}] upload using [{}] ms. file num [{}] .file size [{}] ",
                name, yyyyMMddHHmmss ,
                timeInMills, fileNum ,totalSize );
        //先保存,表示这些文件已经上传到hdfs的临时目录


        try {
            HdfsMeta hdfsMeta = new HdfsMeta(name,yyyyMMddHHmmss,startTime, endTime,
                    fileNum,totalSize,"");
            hdfsDao.create(hdfsMeta);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void uploadAndSaveMeta(String finalRemoteFilePath, List<File> container,String metaDir) {
        List<String> upload = upload(finalRemoteFilePath, container);
        store(metaDir, upload);
    }

    private List<File> findFiles(String pattern) {
        List<File> files = new ArrayList<File>(FileUtils.listFiles(new File(baseWorkPath)   //根据filepattern找到上一小时的日志文件
                , new RegexFileFilter(pattern)
                , TrueFileFilter.INSTANCE));

        Collections.sort(files, new Comparator<File>() {    //根据文件修改日期排序,
            @Override
            public int compare(File o1, File o2) {
                int ret = Long.valueOf(o1.lastModified()).compareTo(o2.lastModified());
                if (ret == 0) {
                    ret = o1.getName().compareTo(o2.getName());
                }
                return ret;
            }
        });
        return files;
    }

    /****
     * 上传本地文件到hdfs.上传前先进行合并.
     * 如果上传成功,则返回任务列表 ; 上传失败,返回空集合
     * @param remoteFilePath
     * @param container
     * @return
     */
    private List<String> upload(final String remoteFilePath, List<File> container) {
        if(container == null || container.size() ==0 ) return new ArrayList<>() ;

        List<String> filePathList = Lists.transform(container, new Function<File, String>() {
            @Nullable
            @Override
            public String apply(@Nullable File file) {
                return file.getAbsolutePath() + ";" + remoteFilePath;
            }
        });

        File firstFile = container.get(0),lastFile = container.get(container.size() -1);

        File output = Files.concat(
                outputFile(this.name + "_" + firstFile.getName() + "_" +  lastFile.getName()),
                container
        );

        if(output.exists()){   //合并成功
            boolean flag = Files.upload(
                    (remoteFilePath.endsWith("/")?remoteFilePath  : remoteFilePath + "/" ) + output.getName() ,
                    output.getAbsolutePath());
            if(flag){   //上传成功
                output.delete();
                log.info("======================upload {} to path {} success.\n{}", output.getAbsoluteFile(), remoteFilePath,
                        StringUtils.join(filePathList, "\n"));
                return filePathList;
            }else{     //上传失败
                log.warn("======================upload {} to path {} failed.\n{}", output.getAbsoluteFile(), remoteFilePath,
                        StringUtils.join(filePathList,"\n"));
                return new ArrayList<>();
            }
        }else{    //合并失败
            log.info("======================failed to concat files {} \n{}",
                    StringUtils.join(filePathList,"\n"));
            return new ArrayList<>();
        }
    }

    private File outputFile(String name){
        if(name == null)
            name = String.valueOf(System.currentTimeMillis()) ;
        return new File(this.tmpDir , name);
    }


    /****
     * 存储增量的更新文件
     * @param metaDir
     * @param delta
     */
    private String store(String metaDir,List<String> delta){
        File metaPath = new File(metaDir, DateFormatUtils.format(new Date(), "yyyyMMddHHmmssSSS"));
        try {
            if(delta==null ) delta = new ArrayList<>();
            FileUtils.writeLines(metaPath,delta);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return metaPath.getAbsolutePath();
    }

    /****
     * 从元数据目录中恢复已处理的文件.
     * @param metaDir
     * @return
     */
    private List<String> restore(String metaDir){
        List<String> processed = new ArrayList<>();
        try {
            File[] fileList = new File(metaDir).listFiles();
            for(File file : fileList){
                processed.addAll(FileUtils.readLines(file));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return processed;
    }

    public static void main(String[] args) {
        DateTime dateTime = Granularity.HOUR.prev(new DateTime());
        DateTimeFormat.forPattern("'/user/zhenchuan.liu/tmp/logs/'yyyy/MM/dd/HH").print(dateTime);
        DateTimeFormat.forPattern("yyyyMMddHH'.*.unbid.log'").print(dateTime);

        for(int i = 0 ; i < 4 ; i++){
            DateTime time = new DateTime().minus(Granularity.HOUR.getUnits(i));
            System.out.println(time);
        }
    }

}
