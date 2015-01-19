package me.zhenchuan.files;

import me.zhenchuan.files.utils.BufferedRandomAccessFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * Created by liuzhenchuan@foxmail.com on 1/15/15.
 */
public abstract class Worker {
    private static final Logger log = LoggerFactory.getLogger(Worker.class);

    private File file;
    private long offset ;

    private String name ;
    private volatile MetaInfo metaInfo ;

    private Overlord overlord;


    public Worker(String name,Overlord overlord){
        this.name = name;
        this.overlord = overlord;
        init();
    }

    protected void init(){

    }

    protected void stop(){

    }

    public void setTask(Task task){
        this.file = new File(task.getFile());
        this.offset = task.getOffset();
        this.metaInfo = new MetaInfo(name,file,offset);
    }

    public void doTask()  {
        if(!file.exists()){
            log.warn("the file {} has been removed before processing.",file.getAbsolutePath());
            reportDone();
            return;
        }
        log.info("worker[{}] begin to process {} at {},{}", this.name, this.file, this.offset,this.file.length());
        long len = file.length();
        if(len < offset){
            log.warn("Log file was reset. Restarting logging from start of file. omit it...{}",file.getAbsolutePath());
            offset = len;
        }else{
            try {
                RandomAccessFile raf = new BufferedRandomAccessFile(file,"r",1024 * 10);
                if(offset!=0)raf.seek(offset);
                String line = null ;
                while((line = raf.readLine())!=null){
                    process(line);
                    metaInfo.reset(name,file,raf.getFilePointer());  //每处理一行,就更新一次metaInfo.
                }
                raf.close();
            } catch (IOException e) {
                e.printStackTrace();
            }finally {
                reportDone();
            }
        }
    }

    public void reportDone(){
        this.overlord.workDone(this);
    }

    public File getFile() {
        return file;
    }

    public void setFile(File file) {
        this.file = file;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    protected abstract void process(String line) ;

    public void checkpoint() {
        if(metaInfo!=null){
            overlord.updateState(name, metaInfo.toString());
        }
    }

    public Overlord getOverlord() {
        return overlord;
    }
}

class MetaInfo {
    String workerName;
    File curFile;
    long curOffset ;

    public MetaInfo(String name, File file, long offset) {
        this.reset(name,file,offset);
    }

    public void reset(String name, File file, long offset){
        this.workerName = name;
        this.curFile = file;
        this.curOffset = offset;
    }

    public String toString(){
        return String.format("%s;%s;%s",workerName,curFile.getAbsoluteFile(),curOffset);
    }

}
