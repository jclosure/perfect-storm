package com.jayway.perfectstorm;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

public class TwitterStreamBuilderUtil {

	public static TwitterStream getStream() {
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true);
		cb.setOAuthConsumerKey(Constants.consumerKey);
		cb.setOAuthConsumerSecret(Constants.consumerSecret);
		cb.setOAuthAccessToken(Constants.accessToken);
		cb.setOAuthAccessTokenSecret(Constants.accessTokenSecret);

		return new TwitterStreamFactory(cb.build()).getInstance();
	}
}