package hello.streams.application;

import java.util.function.Function;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class HelloStreamApp {

	public static void main(String[] args) {
		SpringApplication.run(HelloStreamApp.class, args);
	}

	@Bean
	public Function<String, String> toUpperCase() {
		return value -> {
			System.out.println("Uppercasing " + value);
			return value.toUpperCase();
		};
	}

}
