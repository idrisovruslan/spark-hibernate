package ru.idrisov;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.gson.GsonAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import ru.idrisov.datamart.FirstNewProcessor;

@SpringBootApplication(exclude = {GsonAutoConfiguration.class})
@RequiredArgsConstructor
public class Starter implements CommandLineRunner {

    @Autowired
    FirstNewProcessor firstNewProcessor;

    public static void main(String[] args) {
        new SpringApplicationBuilder(Starter.class)
                .run(args);
    }

    @Override
    public void run(String... args) {

    }
}
