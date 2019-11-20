package com.company.app;

public class StaticMethodClass {
    private static SampleClass s1 = null;

    public static synchronized void initSMC() {
        if (s1 == null) {
            System.out.println(Thread.currentThread().getName() + " gets to initialize s1");
            s1 = new SampleClass();
        }
    }

    public static void displayMsg(){
        System.out.println("In run method " + Thread.currentThread().getName());
        System.out.println(s1);
        for(int i = 0; i < 5 ; i++){
            System.out.println(Thread.currentThread().getName() + " i - " + i);
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
