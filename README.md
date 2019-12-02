# Demo Kafka Streams

### WordCount Demo
<p><strong>Create 2 topics:</strong></p>
<ul>
<li>streams-plaintext-input</li>
<li>streams-wordcount-output</li>
</ul>
<p><strong>Start kafka consumer</strong>: using <code>kafka-console-consumer</code></p>
<pre>kafka-console-consumer --bootstrap-server localhost:9092 \
                       --topic streams-wordcount-output \
                       --from-beginning \
                       --formatter kafka.tools.DefaultMessageFormatter \
                       --property print.key=true \
                       --property print.value=true \
                       --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
                       --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
</pre>
<p><strong>Start kafka streams application</strong>:</p>
<pre>kafka-run-class org.apache.kafka.streams.examples.wordcount.WordCountDemo</pre>
<p><strong>Start kafka producer</strong>: using <code>kafka-console-producer</code> to send some plaintext messages</p>
<pre>kafka-console-producer --broker-list localhost:9092 --topic streams-plaintext-input</pre>
<p>Now you can see what output on <code>kafka consumer</code>, keeping send plaintext message and see output changes</p>
