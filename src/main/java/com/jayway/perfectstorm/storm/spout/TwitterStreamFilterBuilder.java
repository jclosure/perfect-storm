package com.jayway.perfectstorm.storm.spout;

import java.util.stream.Stream;

import twitter4j.FilterQuery;

public class TwitterStreamFilterBuilder {

	FilterQuery query;
	
	public static TwitterStreamFilterBuilder begin(){
		return new TwitterStreamFilterBuilder(new FilterQuery());
	}
	
	public static TwitterStreamFilterBuilder begin(FilterQuery query){
		return new TwitterStreamFilterBuilder(query);
	}
	
	private TwitterStreamFilterBuilder(FilterQuery query) {
		this.query = query;
	}
	
	public FilterQuery build() {
		return query;
	}
	
	public TwitterStreamFilterBuilder filterKeywords(String[] keywords) {
		query.track(keywords);
		return this;
	}
	
	public TwitterStreamFilterBuilder filterLanguage(String[] langs){
	    query.language(langs);
	    return this;
	}

	public TwitterStreamFilterBuilder filterGeography(double[][] boxes){
		query.locations(boxes);
		return this;
	}
	
}
