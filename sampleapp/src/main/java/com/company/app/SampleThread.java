package com.company.app;

class SampleThread implements Runnable{
    private Thread t;

    SampleThread(){
        t = new Thread(this);
        t.start();
    }

    @Override
    public void run() {
        StaticMethodClass.initSMC();
        StaticMethodClass.displayMsg();
    }
}
