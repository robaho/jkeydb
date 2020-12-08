package com.robaho.jkeydb;

public class WaitGroup {
    private int n;
    public synchronized void add(int count) {
        n+=count;
    }

    public synchronized void done() {
        n--;
        notify();
    }

    public synchronized void waitEmpty() {
        while(n>0){
            try {
                wait();
            } catch (InterruptedException e) {
                throw new IllegalStateException("unexpected interrupt");
            }
        }
    }
}
