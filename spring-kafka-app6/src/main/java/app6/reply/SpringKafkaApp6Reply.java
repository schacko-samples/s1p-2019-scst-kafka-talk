package app6.reply;

import org.apache.kafka.clients.admin.NewTopic;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.support.SimpleKafkaHeaderMapper;
import org.springframework.kafka.support.converter.MessagingMessageConverter;
import org.springframework.messaging.handler.annotation.SendTo;

@SpringBootApplication
public class SpringKafkaApp6Reply {

	public static void main(String[] args) {
		SpringApplication app = new SpringApplication(SpringKafkaApp6Reply.class);
		app.setWebApplicationType(WebApplicationType.NONE);
		app.run(args);
	}

	@KafkaListener(id="server", topics = "kRequests")
	@SendTo // use default replyTo expression
	public String listen(String in) {
		System.out.println("Server received: " + in);
		return in.toUpperCase();
	}

	@Bean // not required if Jackson is on the classpath
	public MessagingMessageConverter simpleMapperConverter() {
		MessagingMessageConverter messagingMessageConverter = new MessagingMessageConverter();
		messagingMessageConverter.setHeaderMapper(new SimpleKafkaHeaderMapper());
		return messagingMessageConverter;
	}

	static class Admin {

		@Bean
		public NewTopic kRequests() {
			return TopicBuilder.name("kRequests")
					.partitions(1)
					.replicas(2)
					.build();
		}

		@Bean
		public NewTopic kReplies() {
			return TopicBuilder.name("kReplies")
					.partitions(1)
					.replicas(2)
					.build();
		}
	}

}
