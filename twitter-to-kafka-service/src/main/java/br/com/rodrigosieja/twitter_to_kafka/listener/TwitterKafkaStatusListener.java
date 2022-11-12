package br.com.rodrigosieja.twitter_to_kafka.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import twitter4j.Status;

@Component
public class TwitterKafkaStatusListener extends twitter4j.StatusAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterKafkaStatusListener.class);

    @Override
    public void onStatus(Status status) {
        LOG.info("Twitter status with text {}", status.getText());
    }

}
