package com.university.bigdata.milestone3;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Milestone3Application {

    public static void main(String[] args) {
        Thread schedulerThread = new Thread(new Scheduler(10000));
        schedulerThread.start();
        SpringApplication.run(Milestone3Application.class, args);
    }

}
