package com.company.app;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        System.out.println("Starting Main!");
        SampleThread t1 = new SampleThread();
        SampleThread t2 = new SampleThread();
        SampleThread t3 = new SampleThread();
    }
}
