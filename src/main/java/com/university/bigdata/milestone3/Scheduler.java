package com.university.bigdata.milestone3;

import java.util.ArrayList;
import java.util.List;

public class Scheduler implements Runnable{
    private long interval;
    private List<Obs> subscribers;

    public Scheduler(long interval){
        this.interval = interval;
        this.subscribers = new ArrayList<>();
    }

    public void subscribe(Obs subscriber){
        subscribers.add(subscriber);
    }

    @Override
    public void run() {
        while(true){
            try {
                Thread.sleep(interval);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            System.out.println("Tick!");
            for(Obs subscriber: subscribers)
                subscriber.wakeup();
        }
    }
}
