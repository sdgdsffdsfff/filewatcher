package me.zhenchuan.files;

import org.apache.commons.lang3.time.DateParser;
import org.apache.commons.lang3.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by liuzhenchuan@foxmail.com on 1/15/15.
 */
public class FileModifyTimeFilter implements FileFilter {

    private static final Logger log = LoggerFactory.getLogger(FileModifyTimeFilter.class);

    public static final String PATTERN = "yyyyMMddHHmmss";

    private long lastModify = 1l;

    /****
     * 设置要读取的文件的开始时间.
     * @param dateTime
     */
    public FileModifyTimeFilter(String dateTime){
        if(dateTime == null){
            log.warn("no [acceptModifyTime] has been specified. so it'll process the file .");
            this.lastModify = new Date().getTime();
        }else{
            try {
                this.lastModify = new SimpleDateFormat(PATTERN).parse(dateTime).getTime();
            } catch (ParseException e) {
                throw new IllegalArgumentException("invalid date format[" + PATTERN + "] of " + dateTime,e);
            }
        }

    }

    @Override
    public boolean filter(File file) {
        return (file.lastModified() >= lastModify);
    }
}
