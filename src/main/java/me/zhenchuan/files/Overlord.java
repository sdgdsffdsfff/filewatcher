package me.zhenchuan.files;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import me.zhenchuan.files.utils.LightPauser;
import me.zhenchuan.files.utils.Pauser;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.*;
import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.*;

/**
 * Created by liuzhenchuan@foxmail.com on 1/14/15.
 */
public class Overlord {
    private static final Logger log = LoggerFactory.getLogger(Overlord.class);
    static final String CHECKPOINT_SUFFIX = "_CK" ;
    static final String PROCESSED_SUFFIX = "_ED" ;

    static final String WORKER_PREFIX = "worker-";

    private CountDownLatch stopLatch = new CountDownLatch(1);

    private Queue<Task> filesToProcess ;
    private List<String> filesProcessed;
    private List<String> filesProcessing ;

    private BlockingQueue<Worker> workers ;
    private List<Worker> workersRef ;
    private Map<String,String> checkpoints ;

    private String worker;
    private String name;
    private String baseMetaPath;
    private String baseWorkPath;
    private String fileNamePattern;

    private FileFilter fileFilter;

    //mainly used to init workers...
    private Properties properties ;

    private int parallelism = 4 ;
    private int checkpointFrequencyInSecond = 1 ;
    //重试1次后开始sleep 10s .
    private Pauser pauser = new LightPauser(1,TimeUnit.SECONDS.toNanos(10));

    private volatile boolean running = true;
    private ScheduledExecutorService checkpointingTimer;


    public Overlord(String name, String worker, String baseMetaPath,
                    String baseWorkPath, String fileNamePattern,String acceptModifyTime,
                    int parallelism,
                    int checkpointFrequencyInSecond, Properties properties){
        this.name = name;
        this.worker = worker;
        this.baseMetaPath = baseMetaPath;
        this.baseWorkPath = baseWorkPath;
        assertDir();

        this.fileNamePattern = fileNamePattern;
        this.parallelism = parallelism;
        this.checkpointFrequencyInSecond = checkpointFrequencyInSecond;
        this.fileFilter = new FileModifyTimeFilter(acceptModifyTime);

        this.properties = properties;

        this.filesToProcess = new LinkedList<>();
        this.filesProcessed = new CopyOnWriteArrayList<>();
        this.filesProcessing = new CopyOnWriteArrayList<>();
        this.checkpoints = new ConcurrentHashMap<>(parallelism);

        initWorkers();
        initCheckpointTimer();

        restoreFinishedFileList();
        restoreUnFinishedTask();
    }

    public void assertDir(){
        if(!new File(this.baseMetaPath).exists()){
            new File(this.baseMetaPath).mkdirs();
        }
        if(!new File(this.baseWorkPath).exists()){
            throw new IllegalArgumentException("work dir ["+this.baseWorkPath+"] not exist!");
        }
    }

    public void stop(){
        log.info("Overlord begin to stop work...");
        try {
            running = false;
            stopWorks();
            stopLatch.await(5, TimeUnit.MINUTES);
            checkpointingTimer.shutdownNow();
            checkpointingTimer.awaitTermination(5, TimeUnit.MINUTES);
            persistFinishedState();
            checkpoint(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /****
     * 从checkpoint中恢复上次中断的操作.
     */
    private void restoreUnFinishedTask() {
        try {
            File file = new File(baseMetaPath, this.name + CHECKPOINT_SUFFIX);
            if(!file.exists()){
                log.warn("checkpoint {} not exist!",file.getAbsolutePath());
                return;
            }
            List<String> metaList = FileUtils.readLines(
                    file);
            for(String metaInfo : metaList){
                String[] items = metaInfo.split(";");
                if(items.length == 3){
                    String path = items[1];
                    if(!filesProcessed.contains(path)){   //
                        filesToProcess.add(new Task(path, Integer.parseInt(items[2])));
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void initWorkers(){
        workersRef = new ArrayList<>(parallelism);
        workers = new ArrayBlockingQueue<>(parallelism);
        for(int i = 1 ; i <= parallelism ;i++){
            String workerName = WORKER_PREFIX + i;
            Worker worker = buildWorker(workerName, this);
            if(worker == null){
                throw new RuntimeException("can not init worker [" + worker + "]");
            }
            workers.add(worker);
            workersRef.add(worker);
        }
    }

    private Worker buildWorker(String workerName,Overlord overlord){

        try {
            Class[] paramType = {String.class,Overlord.class} ;
            Class<Worker> clazz = (Class<Worker>) Class.forName(worker);
            Constructor<Worker> constructor = clazz.getConstructor(paramType);
            return constructor.newInstance(workerName,overlord);
        }catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public void initCheckpointTimer(){
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setDaemon(true)
                .setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {

                    @Override
                    public void uncaughtException(Thread t, Throwable e) {
                        log.error("Expection from checkpointing thread", e);
                    }
                }).setNameFormat("Checkpointing-trigger-" + getClass().getSimpleName()).build();
        checkpointingTimer = Executors.newSingleThreadScheduledExecutor(threadFactory);
        checkpointingTimer.scheduleAtFixedRate(
                new CheckpointingTask(this), checkpointFrequencyInSecond,
                checkpointFrequencyInSecond, TimeUnit.SECONDS
        );
    }

    public void dispatch(){
        while(running){
            try {
                final Task task = pickTask();
                if(task ==  null){
                    log.warn("failed to pick a task,may be the Overlord not running. cur state [" + running + "]");
                    break;
                }
                final Worker worker = workers.take();
                //FIXME 在添加时,这块的信息可能在workDone已经发生变化.
                boolean flag = this.getFilesProcessing().add(task.getFile());
                worker.setTask(task);
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        worker.doTask();
                    }
                }).start();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        stopLatch.countDown();
    }

    public Task pickTask() {
        Task task = filesToProcess.poll();
        while(task == null && running){
            fillNewTasks();
            if(filesToProcess.size() > 0){
                task = filesToProcess.poll();
                pauser.unpause();
            }else{
                pauser.pause();
            }
        }
        return task;
    }

    private void restoreFinishedFileList() {
        try {
            File file = new File(baseMetaPath, this.name + PROCESSED_SUFFIX);
            if(!file.exists()){
                log.warn("FinishedFileList {} not exist!",file.getAbsolutePath());
                return;
            }
            List<String> list = FileUtils.readLines(file);
            for(String path : list){
                File f = new File(path);
                if(f.exists()){ //这里文件rsync时可能已经被删除了...
                    filesProcessed.add(path);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void persistFinishedState(){
        try {
            FileUtils.writeLines(new File(baseMetaPath,this.name  + PROCESSED_SUFFIX ),filesProcessed);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /****
     * 对已经删除的文件进行清理.
     */
    public  void clearStaleFinishedState(){
        for(String path : filesProcessed){
            File f = new File(path);
            if(!f.exists()){
                filesProcessed.remove(path);
            }
        }
    }

    /****
     * 这个方法只有在当前filesToProcess都为空时才进行调用.
     */
    public void fillNewTasks(){
        long s = System.currentTimeMillis();
        Collection<File> files = getFiles();
        for(File file : files){
            if(file.isFile() && filter(file)){
                String path = file.getAbsolutePath();
                if(!filesProcessed.contains(path) && !filesProcessing.contains(path)){
                    filesToProcess.offer(new Task(path,0));
                }
            }
        }
        log.info("found {} files to be processed...using {}ms",filesToProcess.size(),(System.currentTimeMillis() - s));
    }

    private Collection<File> getFiles() {
        return  FileUtils.listFiles(new File(this.baseWorkPath)
                , new RegexFileFilter(fileNamePattern)
                , TrueFileFilter.INSTANCE);
    }

    private boolean filter(File file){
        return fileFilter.filter(file);
    }

    /****
     * 更新worker的进度
     * @param workerName
     * @param value
     */
    public void updateState(String workerName, String value) {
        checkpoints.put(workerName, value);
    }

    public void stopWorks(){
        for(Worker worker : workersRef){
            worker.stop();
        }
    }

    /****
     * 持久化进度信息.
     * @param index
     */
    public void checkpoint(int index){
        Collection<String> lastCheckPoints = new HashSet<>(checkpoints.values());
        //bug: 这里workers可能可能都被take了.
        for(Worker worker : workersRef){
            worker.checkpoint();
        }
        Collection<String> values = new HashSet<>(checkpoints.values());
        if(lastCheckPoints.equals(values)){
            log.info("filesProcessed[{}];filesToProcess[{}];filesProcessing[{}];doneNum[{}] \nthe meta info has no change between the checkpoint interval.",
                    this.filesProcessed.size(),this.filesToProcess.size(),
                    this.filesProcessing.size(),this.doneNum
                    );
            return;
        }
        try {
            //FIXME : 重写操作非原子,使用index++ 新建还是比较好一点
            FileUtils.writeLines(new File(getBaseMetaPath(),
                    getName() +  Overlord.CHECKPOINT_SUFFIX),values);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private int doneNum = 1 ;
    public synchronized void workDone(Worker worker) {
        this.getFilesProcessed().add(worker.getFile().getAbsolutePath());
        boolean flag = this.getFilesProcessing().remove(worker.getFile().getAbsolutePath());
        if(flag==false){
            log.warn("add file failed" + worker.getFile().getAbsolutePath() + "\t" + worker.getName() + "\n" + this.getFilesProcessing());
        }
        if(doneNum ++ % 1000 == 0 ){    //每隔1000次进行一次清除
            long s = this.filesProcessed.size() ;
            this.clearStaleFinishedState();
            long e = this.filesProcessed.size();
            log.info("after doneNum[{}] clear stale files. [{}],[{}]",doneNum,s,e);
        }
        this.persistFinishedState();
        try {
            workers.put(worker);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public String getName() {
        return name;
    }

    public String getBaseMetaPath() {
        return baseMetaPath;
    }

    public String getBaseWorkPath() {
        return baseWorkPath;
    }


    public List<String> getFilesProcessed() {
        return filesProcessed;
    }

    public List<String> getFilesProcessing() {
        return filesProcessing;
    }


    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    /****
     * 获取设置的属性可以通过 System.getProperty() 来代替.
     * @return
     */
    @Deprecated
    public Properties getProperties() {
        return properties;
    }
}

class CheckpointingTask extends TimerTask{

    private Overlord overlord ;

    private volatile int index = 0;

    public CheckpointingTask(Overlord overlord){
        this.overlord = overlord ;
    }

    @Override
    public void run() {
        overlord.checkpoint(index ++ );
    }
}

