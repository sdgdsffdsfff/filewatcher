package me.zhenchuan.files.ext;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import com.google.common.base.Preconditions;
import me.zhenchuan.files.Overlord;
import me.zhenchuan.files.Worker;

import java.util.Properties;

/**
 * Created by liuzhenchuan@foxmail.com on 1/19/15.
 */
public class RocketMqWorker extends Worker{

    private static DefaultMQProducer producer;
    private String topic;

    static {
        String producerGroup = System.getProperty("rocketmq.producer.group");
        String brokerList = System.getProperty("rocketmq.server.list");

        Preconditions.checkNotNull(producerGroup);
        Preconditions.checkNotNull(brokerList);

        producer = new DefaultMQProducer(producerGroup) ;
        producer.setNamesrvAddr(brokerList);

        try {
            producer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
            throw new RuntimeException("failed to start rocketmq producer.");
        }
    }

    public RocketMqWorker(String name, Overlord overlord) {
        super(name, overlord);
    }

    @Override
    protected void init() {
        super.init();
        this.topic = System.getProperty("rocketmq.topic");
        Preconditions.checkNotNull(this.topic);
    }

    @Override
    protected void stop() {
        super.stop();
        producer.shutdown();
    }

    @Override
    protected void process(String line) {
        try {
            producer.sendOneway(new Message(this.topic,line.getBytes()));
        } catch (MQClientException e) {
            e.printStackTrace();
        } catch (RemotingException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


}
