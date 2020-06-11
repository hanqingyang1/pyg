package com.itheima.report;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class KafkTest {
    @Autowired
    private KafkaTemplate kafkaTemplate;

    @Test
    public void sendTest01() {
        for(int i = 0; i < 100; i++) {
            kafkaTemplate.send("test",  "test msg!");
        }
    }
}