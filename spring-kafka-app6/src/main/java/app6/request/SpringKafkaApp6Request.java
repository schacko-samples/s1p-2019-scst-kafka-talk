package app6.request;

import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Lazy;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
public class SpringKafkaApp6Request {

	public static void main(String[] args) {
		SpringApplication.run(SpringKafkaApp6Request.class, args);
	}

	@RestController
	static class Producer {

		@Autowired @Lazy ReplyingKafkaTemplate<String, String, String> replyingTemplate;

		@PostMapping(path = "/publish/demo")
		public String publishDemo(@RequestBody String data) throws Exception {
			ProducerRecord<String, String> record = new ProducerRecord<>("kRequests", data);
			RequestReplyFuture<String, String, String> replyFuture = replyingTemplate.sendAndReceive(record);
			SendResult<String, String> sendResult = replyFuture.getSendFuture().get(10, TimeUnit.SECONDS);
			System.out.println("Sent ok: " + sendResult.getRecordMetadata());

			ConsumerRecord<String, String> consumerRecord = replyFuture.get(120, TimeUnit.SECONDS);
			System.out.println("Return value: " + consumerRecord.value());
			return consumerRecord.value();
		}

		@Bean
		public ReplyingKafkaTemplate<String, String, String> replyingTemplate(
				ProducerFactory<String, String> pf,
				ConcurrentMessageListenerContainer<String, String> repliesContainer) {
			return new ReplyingKafkaTemplate<>(pf, repliesContainer);
		}

		@Bean
		public ConcurrentMessageListenerContainer<String, String> repliesContainer(
				ConcurrentKafkaListenerContainerFactory<String, String> containerFactory) {
			ConcurrentMessageListenerContainer<String, String> repliesContainer =
					containerFactory.createContainer("kReplies");
			repliesContainer.getContainerProperties().setGroupId("repliesGroup");
			repliesContainer.setAutoStartup(false);
			return repliesContainer;
		}
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
