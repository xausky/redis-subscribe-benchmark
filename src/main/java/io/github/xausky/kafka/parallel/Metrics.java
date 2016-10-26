package io.github.xausky.kafka.parallel;

/**
 * Created by xausky on 10/20/16.
 */
public class Metrics {
    private Integer producerCount = 0;
    private Integer consumerCount = 0;

    public void produced(){
        synchronized (producerCount){
            producerCount++;
        }
    }

    public void consumed(){
        synchronized (consumerCount){
            consumerCount++;
        }
    }

    public void print(){
        System.out.printf("consumerCount:%d,producerCount:%d\n",consumerCount,producerCount);
    }
}
