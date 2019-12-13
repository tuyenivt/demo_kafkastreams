package com.coloza.demo.kafkastreams;

public class FavouriteColourStreamsAppRunner {

    public static void main(String[] args) {
        FavouriteColourStreamsApp app = new FavouriteColourStreamsApp("localhost:9092");
        // app.transform("favouritecolour-streams-input", "favouritecolour-streams-output");
        app.transformWithIntermediaryTopic("favouritecolour-streams-input", "favouritecolour-streams-output");
    }
}
