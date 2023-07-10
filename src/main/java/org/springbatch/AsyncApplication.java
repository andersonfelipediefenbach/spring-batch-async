package org.springbatch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class AsyncApplication {
    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(AsyncApplication.class, args);
        context.close();
    }

}