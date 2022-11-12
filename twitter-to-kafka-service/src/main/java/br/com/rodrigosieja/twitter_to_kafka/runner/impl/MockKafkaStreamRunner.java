package br.com.rodrigosieja.twitter_to_kafka.runner.impl;

import br.com.rodrigosieja.twitter_to_kafka.config.TwitterToKafkaServiceConfiguration;
import br.com.rodrigosieja.twitter_to_kafka.exception.TwitterToKafkaServiceException;
import br.com.rodrigosieja.twitter_to_kafka.listener.TwitterKafkaStatusListener;
import br.com.rodrigosieja.twitter_to_kafka.runner.StreamRunner;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

@Component
@ConditionalOnProperty(name = "twitter-to-kafka-service.enable-mock-tweets", havingValue = "true")
@Slf4j
public class MockKafkaStreamRunner implements StreamRunner {

    private final TwitterToKafkaServiceConfiguration twitterConfiguration;
    private final TwitterKafkaStatusListener twitterKafkaStatusListener;

    private static final Random RANDOM = new Random();
    private static final String[] WORDS = new String[] {
            "lorem",
            "ipsum",
            "dolor",
            "sit",
            "amet"
    };
    private static final String RAW_JSON_TWEET = "{" +
            "\"created_at\":\"{0}\"," +
            "\"id\":\"{1}\"," +
            "\"text\":\"{2}\"," +
            "\"user\":{\"id\":\"{3}\"}" +
            "}";

    private static final String TWITTER_STATUS_DATE_FORMAT = "EEE MMM dd HH:mm:ss zzz yyyy";

    public MockKafkaStreamRunner(TwitterToKafkaServiceConfiguration twitterConfiguration,
                                 TwitterKafkaStatusListener twitterKafkaStatusListener) {
        this.twitterConfiguration = twitterConfiguration;
        this.twitterKafkaStatusListener = twitterKafkaStatusListener;
    }

    @Override
    public void start() throws TwitterException {
        String[] keywords = twitterConfiguration.getTwitterKeywords().toArray(new String[0]);
        Integer mockMinTweetLength = twitterConfiguration.getMockMinTweetLength();
        Integer mockMaxTweetLength = twitterConfiguration.getMockMaxTweetLength();
        Long mockSleepMs = twitterConfiguration.getMockSleepMs();

        log.info("Started mock filtering for keywords {}", Arrays.toString(keywords));

        simulateTwitterStream(keywords, mockMinTweetLength, mockMaxTweetLength, mockSleepMs);
    }

    private void simulateTwitterStream(final String[] keywords,
                                       final Integer mockMinTweetLength,
                                       final Integer mockMaxTweetLength,
                                       final Long mockSleepMs) {
        Executors.newSingleThreadExecutor().submit(() -> {
            try {
                while (true) {
                    String formattedTweetAsJson = getFormattedTweet(keywords, mockMinTweetLength, mockMaxTweetLength);
                    Status status = TwitterObjectFactory.createStatus(formattedTweetAsJson);
                    twitterKafkaStatusListener.onStatus(status);
                    sleep(mockSleepMs);
                }
            } catch (TwitterException ex) {
                log.error("Error on creating Twitter status!", ex);
            }
        });
    }

    private void sleep(long sleepTimeMs) {
        try {
            Thread.sleep(sleepTimeMs);
        } catch (InterruptedException ex) {
            throw new TwitterToKafkaServiceException("Error while sleeping");
        }
    }

    private String getFormattedTweet(final String[] keywords,
                                     final Integer mockMinTweetLength,
                                     final Integer mockMaxTweetLength) {
        String[] params = new String[] {
                ZonedDateTime.now().format(DateTimeFormatter.ofPattern(TWITTER_STATUS_DATE_FORMAT, Locale.ENGLISH)),
                String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)),
                getRandomTweetContent(keywords, mockMinTweetLength, mockMaxTweetLength),
                String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)),
        };

        return formatTweetAsJson(params);
    }

    private static String formatTweetAsJson(String[] params) {
        String tweet = RAW_JSON_TWEET;

        for (int i = 0; i < params.length; i++) {
            tweet = tweet.replace("{" + i + "}", params[i]);
        }

        return tweet;
    }

    private String getRandomTweetContent(final String[] keywords,
                                         final Integer mockMinTweetLength,
                                         final Integer mockMaxTweetLength) {
        StringBuilder tweet = new StringBuilder();

        int tweetLength = RANDOM.nextInt(mockMaxTweetLength - mockMinTweetLength + 1) + mockMinTweetLength;
        return constructRandomTweet(keywords, tweet, tweetLength);
    }

    private static String constructRandomTweet(final String[] keywords,
                                               final StringBuilder tweet,
                                               final int tweetLength) {
        for (int i = 0; i < tweetLength; i++) {
            tweet.append(WORDS[RANDOM.nextInt(WORDS.length)]).append(" ");
            if (i == tweetLength / 2) {
                tweet.append(keywords[RANDOM.nextInt(keywords.length)]).append(" ");
            }
        }

        return tweet.toString().trim();
    }
}
