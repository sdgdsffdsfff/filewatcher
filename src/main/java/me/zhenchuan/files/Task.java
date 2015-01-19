package me.zhenchuan.files;

/**
 * Created by liuzhenchuan@foxmail.com on 1/15/15.
 */
public class Task {

    private String file;
    private int offset ;

    public Task(String file, int offset) {
        this.file = file;
        this.offset = offset;
    }

    public String getFile() {
        return file;
    }

    public void setFile(String file) {
        this.file = file;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    @Override
    public String toString() {
        return "Task{" +
                "file='" + file + '\'' +
                ", offset=" + offset +
                '}';
    }
}
