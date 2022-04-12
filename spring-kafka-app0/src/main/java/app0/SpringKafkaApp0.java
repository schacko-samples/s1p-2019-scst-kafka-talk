package app0;

import com.github.javafaker.Book;
import com.github.javafaker.Faker;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;

@SpringBootApplication
public class SpringKafkaApp0 {

	private static final Logger logger = LoggerFactory.getLogger(SpringKafkaApp0.class);

	public static void main(String[] args) {
		SpringApplication.run(SpringKafkaApp0.class, args);
	}

		static class Producer {

		@Bean
		public ApplicationRunner runner(KafkaTemplate<String, String> kafkaTemplate) {
			Faker faker = Faker.instance();
			return args -> {
				for (int i = 0; i < 100; i++) {
					final Book book = faker.book();
					final ListenableFuture<SendResult<String, String>> send = kafkaTemplate.send("spring-kafka-app0-demo1",
							String.join(", ", book.title(), book.author(), book.genre(), book.publisher()));
					final SendResult<String, String> stringStringSendResult = send.get();
					Thread.sleep(100);
				}
			};
		}
	}

	static class Admin {

		@Bean
		public NewTopic quickTopic() {
			return TopicBuilder.name("spring-kafka-app0-demo1")
					.partitions(1)
					.replicas(3)
					.build();
		}

	}

	@Component
	static class Listener {

		@KafkaListener(id = "sk-app0-demo1-group", topics = "spring-kafka-app0-demo1")
		public void listen(String in) {
			logger.info("Data Received : " + in);
		}

	}

}