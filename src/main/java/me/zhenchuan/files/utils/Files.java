package me.zhenchuan.files.utils;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by liuzhenchuan@foxmail.com on 1/22/15.
 */
public class Files {

    public static File concat(final File output ,List<File> fileList){
        try{
            ByteBuffer[] byteBuffers = new ByteBuffer[fileList.size()];
            for(int i = 0 ; i < fileList.size() ;i++){
                RandomAccessFile raf = new RandomAccessFile(fileList.get(i),"r");
                FileChannel channel = raf.getChannel();
                byteBuffers[i] = channel.map(FileChannel.MapMode.READ_ONLY,0,raf.length());
                channel.close();
            }
            FileOutputStream fileOutputStream = new FileOutputStream(output);
            FileChannel fileChannel = fileOutputStream.getChannel();
            fileChannel.write(byteBuffers);
            fileChannel.close();
        }catch (IOException e){
            output.delete();
        }
        return output;
    }

    public static boolean upload(String remoteFilePath,String localFilePath){
        boolean success = false ;
        try {
            Configuration hadoopConf = new Configuration();
            hadoopConf.set("fs.hdfs.impl",
                    org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
            );
            hadoopConf.set("fs.file.impl",
                    org.apache.hadoop.fs.LocalFileSystem.class.getName()
            );
            Path outFile = new Path(remoteFilePath);
            FileSystem fs = outFile.getFileSystem(hadoopConf);
            fs.mkdirs(outFile.getParent());
            fs.moveFromLocalFile(new Path(localFilePath), outFile);
            success = true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return success;
    }

}
