package me.zhenchuan.files;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class OverlordBuilder {
    private String name;
    private String worker;
    private String baseMetaPath;
    private String baseWorkPath;
    private String fileNamePattern;
    private String acceptModifyTime;
    private int parallelism;
    private int checkpointFrequencyInSecond;
    private Properties properties ;

    public OverlordBuilder setName(String name) {
        this.name = name;
        return this;
    }

    public OverlordBuilder setBaseMetaPath(String baseMetaPath) {
        this.baseMetaPath = baseMetaPath;
        return this;
    }

    public OverlordBuilder setBaseWorkPath(String baseWorkPath) {
        this.baseWorkPath = baseWorkPath;
        return this;
    }

    public OverlordBuilder setFileNamePattern(String fileNamePattern) {
        this.fileNamePattern = fileNamePattern;
        return this;
    }

    public OverlordBuilder setParallelism(int parallelism) {
        this.parallelism = parallelism;
        return this;
    }

    public OverlordBuilder setWorker(String worker) {
        this.worker = worker;
        return this;
    }

    public OverlordBuilder setAcceptModifyTime(String acceptModifyTime) {
        this.acceptModifyTime = acceptModifyTime;
        return this;
    }

    public OverlordBuilder setProperties(String file) {
        if(file == null || file.length() == 0){
            this.properties = new Properties();
        }else{
            try {
                Properties properties = new Properties();
                properties.load(new FileInputStream(new File(file)));
                this.properties = properties;
                //添加到系统属性(用于singleton)
                for(String key : properties.stringPropertyNames()){
                    System.setProperty( key,properties.getProperty(key));
                }
            } catch (IOException e) {
                throw new IllegalArgumentException("failed to load property file [" + file + "]");
            }
        }
        return this;
    }

    public OverlordBuilder setCheckpointFrequencyInSecond(int checkpointFrequencyInSecond) {
        this.checkpointFrequencyInSecond = checkpointFrequencyInSecond;
        return this;
    }

    public Overlord createOverlord() {
        return new Overlord(name,worker,
                baseMetaPath, baseWorkPath,
                fileNamePattern,acceptModifyTime,
                parallelism, checkpointFrequencyInSecond,properties);
    }
}