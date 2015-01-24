package me.zhenchuan.files.hdfs;

import com.j256.ormlite.field.DatabaseField;
import com.j256.ormlite.table.DatabaseTable;

/**
 * Created by liuzhenchuan@foxmail.com on 1/23/15.
 */
@DatabaseTable(tableName = "hdfs_meta")
public class HdfsMeta {

    @DatabaseField(generatedId = true)
    private int id;

    @DatabaseField
    private String app;

    @DatabaseField
    private String gran;

    @DatabaseField
    private long startTime ;

    @DatabaseField
    private long endTime;

    @DatabaseField
    private int fileNum;

    @DatabaseField
    private long fileSize;

    @DatabaseField(width = 1024)
    private String info ;


    public HdfsMeta() {

    }

    public HdfsMeta(String app, String gran, long startTime,long endTime, int fileNum, long fileSize, String info) {
        this.app = app;
        this.gran = gran;
        this.startTime =startTime;
        this.endTime = endTime;
        this.fileNum = fileNum;
        this.fileSize = fileSize;
        this.info = info;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getApp() {
        return app;
    }

    public void setApp(String app) {
        this.app = app;
    }

    public String getGran() {
        return gran;
    }

    public void setGran(String gran) {
        this.gran = gran;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public int getFileNum() {
        return fileNum;
    }

    public void setFileNum(int fileNum) {
        this.fileNum = fileNum;
    }

    public long getFileSize() {
        return fileSize;
    }

    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }

    public String getInfo() {
        return info;
    }

    public void setInfo(String info) {
        this.info = info;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }
}
