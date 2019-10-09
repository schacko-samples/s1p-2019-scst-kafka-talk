package user.clicks.region.producer.userclicksregionproducer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.MessageChannel;
import org.springframework.util.DigestUtils;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
@EnableBinding(UserDataProeuder.class)
@RestController
public class UserClicksRegionProducerApplication {

	@Autowired
	UserDataProeuder userDataProeuder;

	public static void main(String[] args) {
		SpringApplication.run(UserClicksRegionProducerApplication.class, args);
	}

	@RequestMapping(value = "/user-region/{user}/{region}", method = RequestMethod.POST)
	@ResponseBody
	public void region(@PathVariable("user") String user, @PathVariable("region") String region) {
		userDataProeuder.region().send(MessageBuilder.withPayload(region)
				.setHeader(KafkaHeaders.MESSAGE_KEY, user).build());
	}

	@RequestMapping(value = "/user-clicks/{user}/{clicks}", method = RequestMethod.POST)
	@ResponseBody
	public void clicks(@PathVariable("user") String user, @PathVariable("clicks") long clicks) {
		userDataProeuder.clicks().send(MessageBuilder.withPayload(clicks)
				.setHeader(KafkaHeaders.MESSAGE_KEY, user).build());
	}

}

interface UserDataProeuder {

	String REGION = "regions";
	String CLICKS = "clicks";

	@Output(REGION)
	MessageChannel region();

	@Output(CLICKS)
	MessageChannel clicks();
}
