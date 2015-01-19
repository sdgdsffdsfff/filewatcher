package me.zhenchuan.files;

import java.io.File;

/**
 * Created by liuzhenchuan@foxmail.com on 1/15/15.
 */
public interface FileFilter {
    public boolean filter(File file);
}
