package com.university.bigdata.milestone3;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Milestone3Application {

    public static Scheduler scheduler = new Scheduler(100000);

    public static void main(String[] args) {
        Thread schedulerThread = new Thread(scheduler);
        schedulerThread.start();
        SpeedLayer speedLayer = new SpeedLayer();
        scheduler.subscribe(speedLayer);
        SpringApplication.run(Milestone3Application.class, args);
    }

}
