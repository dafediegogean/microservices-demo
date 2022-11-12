package br.com.rodrigosieja.twitter_to_kafka.runner;

import twitter4j.TwitterException;

public interface StreamRunner {

    void start() throws TwitterException;

}
