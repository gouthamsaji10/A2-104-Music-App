package com.a2.assignment.music;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class MusicApiServer {

    public static void main(String[] args) {
        /*
         Starting point of the backend app
         When the project is deployed on EC2 or ECS, this starts the Spring Boot API server
         */
        SpringApplication.run(MusicApiServer.class, args);
    }
}