package com.coloza.demo.kafkastreams;

public class WordCountStreamsAppTests {

    public static void main(String[] args) {
        WordCountStreamsApp app = new WordCountStreamsApp("localhost:9092");
        app.stream("streams-plaintext-input", "streams-wordcount-output");
    }
}
