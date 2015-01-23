package me.zhenchuan.files.hdfs;

import org.apache.commons.lang3.builder.ToStringBuilder;

public class HdfsFileBatchBuilder {

    private String name = "xxx-unbid";
    private int safeInterval = 4 ;

    private String baseWorkPath = "/data";
    private String tmpDir = "/tmp/";
    private String filenamePattern = "yyyyMMddHH'.*.unbid.log'";
    private String hdfsPathPattern = "/user/zhenchuan.liu/logs/yyyy/MM/dd/hh";
    private String gran = "HOUR";
    private long maxUploadSize = 200 * 1024 * 1024;


    public HdfsFileBatchBuilder setBaseWorkPath(String baseWorkPath) {
        this.baseWorkPath = baseWorkPath;
        return this;
    }

    public HdfsFileBatchBuilder setTmpDir(String tmpDir) {
        this.tmpDir = tmpDir;
        if(!this.tmpDir.endsWith("/")) {
            this.tmpDir = this.tmpDir + "/";
        }
        return this;
    }

    public HdfsFileBatchBuilder setFilenamePattern(String filenamePattern) {
        this.filenamePattern = filenamePattern;
        return this;
    }

    public HdfsFileBatchBuilder setHdfsPathPattern(String hdfsPathPattern) {
        this.hdfsPathPattern = hdfsPathPattern;
        return this;
    }

    public HdfsFileBatchBuilder setGran(String gran) {
        this.gran = gran;
        return this;
    }

    public HdfsFileBatchBuilder setMaxUploadSize(long maxUploadSize) {
        this.maxUploadSize = maxUploadSize;
        return this;
    }

    public HdfsFileBatchBuilder setName(String name) {
        this.name = name;
        return this;
    }

    public HdfsFileBatchBuilder setSafeInterval(int safeInterval) {
        this.safeInterval = safeInterval;
        return this;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("name", name)
                .append("baseWorkPath", baseWorkPath)
                .append("tmpDir", tmpDir)
                .append("filenamePattern", filenamePattern)
                .append("hdfsPathPattern", hdfsPathPattern)
                .append("gran", gran)
                .append("maxUploadSize", maxUploadSize)
                .append("safeInterval", safeInterval)
                .toString();
    }

    public HdfsFileBatch createHdfsFileBatch() {
        return new HdfsFileBatch(name,baseWorkPath, tmpDir, filenamePattern,
                hdfsPathPattern, gran, maxUploadSize,safeInterval);
    }
}