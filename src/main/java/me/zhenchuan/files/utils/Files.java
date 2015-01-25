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
            Configuration hadoopConf = createHadoopConfiguration();
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

    public static boolean exist(String path){
        boolean success = false;
        try {
            Configuration hadoopConf = createHadoopConfiguration();
            FileSystem fileSystem = FileSystem.get(hadoopConf);
            return fileSystem.exists(new Path(path));
        }catch (Exception e){

        }
        return success;
    }

    public static boolean delete(String remote){
        boolean success = false;
        try {
            Configuration hadoopConf = createHadoopConfiguration();
            FileSystem fileSystem = FileSystem.get(hadoopConf);
            fileSystem.delete(new Path(remote),true);
            success = true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return success;
    }

    public static boolean move(String sourcePath,String target){
        boolean success = false;
        try {
            Configuration hadoopConf = createHadoopConfiguration();
            FileSystem fileSystem = FileSystem.get(hadoopConf);

            Path src = new Path(sourcePath);
            Path dst = new Path(target);
            fileSystem.mkdirs(src);
            fileSystem.mkdirs(dst);

            RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(src,false);
            while(iterator.hasNext()){
                Path source = iterator.next().getPath();
                //TODO try fileSystem.rename
                //这样又是老问题了,依然无法保证这个copy过程中出错,下次同步文件变多的情况.
                fileSystem.rename(source,new Path(target,source.getName()));
                //FileUtil.copy(fileSystem, source,fileSystem, dst,true,hadoopConf);
            }

            success = true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return success;
    }

    private static Configuration createHadoopConfiguration() {
        Configuration hadoopConf = new Configuration();
        hadoopConf.set("fs.hdfs.impl",
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
        );
        hadoopConf.set("fs.file.impl",
                LocalFileSystem.class.getName()
        );
        return hadoopConf;
    }

}
