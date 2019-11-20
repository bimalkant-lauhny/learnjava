package com.company.app;

import java.util.Random;

public class SampleClass {
    public SampleClass() {
        System.out.println("SampleClass object accessed by: " + Thread.currentThread().getName());
        try {
            Random rand = new Random();
            int sleepTime = (rand.nextInt(1000)) * 10;
            System.out.println("sleepTime: " + sleepTime);
            Thread.sleep(sleepTime);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
