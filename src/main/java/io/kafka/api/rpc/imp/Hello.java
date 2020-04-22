package io.kafka.api.rpc.imp;

import io.kafka.api.rpc.IHello;

import java.util.Date;

public class Hello implements IHello {

    @Override
    public String sayHello(String name, int age) {
        System.out.println("bean hello sayHello Invoke..");
        return "hello," + name + ",your age is " + age;
    }

    @Override
    public int add(int a, int b) {
        return 0;
    }

    @Override
    public Date getDate() {
        return null;
    }
}
