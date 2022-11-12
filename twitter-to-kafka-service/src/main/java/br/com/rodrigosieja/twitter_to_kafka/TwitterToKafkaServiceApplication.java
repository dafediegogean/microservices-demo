package br.com.rodrigosieja.twitter_to_kafka;

import br.com.rodrigosieja.twitter_to_kafka.config.TwitterToKafkaServiceConfiguration;
import br.com.rodrigosieja.twitter_to_kafka.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import java.util.Arrays;

@SpringBootApplication
@ComponentScan("br.com.rodrigosieja")
public class TwitterToKafkaServiceApplication implements CommandLineRunner {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterToKafkaServiceApplication.class);

    private final TwitterToKafkaServiceConfiguration twitterToKafkaServiceConfiguration;

    private final StreamRunner streamRunner;

    public TwitterToKafkaServiceApplication(TwitterToKafkaServiceConfiguration twitterToKafkaServiceConfiguration,
                                            StreamRunner streamRunner) {
        this.twitterToKafkaServiceConfiguration = twitterToKafkaServiceConfiguration;
        this.streamRunner = streamRunner;
    }

    public static void main(String[] args) {
        SpringApplication.run(TwitterToKafkaServiceApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        LOG.info("TwitterToKafkaService starts");
        LOG.info(Arrays.toString(twitterToKafkaServiceConfiguration.getTwitterKeywords().toArray(new String[] {})));
        LOG.info("Welcome message: {}", twitterToKafkaServiceConfiguration.getWelcomeMessage());

        streamRunner.start();
    }
}
