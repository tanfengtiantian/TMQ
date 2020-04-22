package io.kafka.api.rpc;

import java.util.Date;

public interface IHello {
     String sayHello(String name, int age);
     int add(int a, int b);
     Date getDate();
}
