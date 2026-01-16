package com.polyglot.sms.sender;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class SmsSenderApplication {

	public static void main(String[] args) {
		SpringApplication.run(SmsSenderApplication.class, args);
		System.out.println("SmsSenderApplication started");
	}

}
