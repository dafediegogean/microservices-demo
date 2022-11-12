package br.com.rodrigosieja.twitter_to_kafka.runner.impl;

import br.com.rodrigosieja.twitter_to_kafka.config.TwitterToKafkaServiceConfiguration;
import br.com.rodrigosieja.twitter_to_kafka.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@ConditionalOnExpression("${twitter-to-kafka-service.enable-v2-tweets} && not ${twitter-to-kafka-service.enable-mock-tweets}")
public class TwitterV2KafkaStreamRunner implements StreamRunner {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterV2KafkaStreamRunner.class);

    private final TwitterToKafkaServiceConfiguration twitterConfiguration;

    private final TwitterV2StreamHelper streamHelper;

    public TwitterV2KafkaStreamRunner(TwitterToKafkaServiceConfiguration twitterConfiguration,
                                      TwitterV2StreamHelper streamHelper) {
        this.twitterConfiguration = twitterConfiguration;
        this.streamHelper = streamHelper;
    }

    @Override
    public void start() {
        String bearerToken = twitterConfiguration.getTwitterV2BearerToken();

        if (null != bearerToken) {
            try {
                streamHelper.setupRules(bearerToken, getRules());
                streamHelper.connectStream(bearerToken);
            } catch (IOException | URISyntaxException e) {
                LOG.error("Error streaming tweets!", e);
                throw new RuntimeException("Error streaming tweets!", e);
            }
        } else {
            LOG.error("There was a problem getting your bearer token. " +
                    "Please make sure you set the TWITTER_BEARER_TOKEN environment variable");
            throw new RuntimeException("There was a problem getting your bearer token. +" +
                    "Please make sure you set the TWITTER_BEARER_TOKEN environment variable");
        }

    }

    private Map<String, String> getRules() {
        List<String> keywords = twitterConfiguration.getTwitterKeywords();

        Map<String, String> rules = new HashMap<>();
        keywords.forEach(k -> rules.put(k, "keyword: " + k));
        LOG.info("Created filter for twitter stream for keywords: {}", keywords);

        return rules;
    }
}
