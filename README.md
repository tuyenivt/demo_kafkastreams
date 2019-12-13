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
<pre>kafka-run-class com.coloza.demo.kafkastreams.WordCountStreamsApp</pre>
<p><strong>Start kafka producer</strong>: using <code>kafka-console-producer</code> to send some plaintext messages</p>
<pre>kafka-console-producer --broker-list localhost:9092 --topic streams-plaintext-input</pre>
<p>Now you can see what output on <code>kafka consumer</code>, keeping send plaintext message and see output changes</p>

### User and Favourite Colour Demo
#### Description
<p>Write a stream application, that</p>
<ul>
<li>Take a comma delimited topic of user,colour
<ul>
<li>Filter out bad data</li>
<li>Keep only colour of "red", "blue", or "green"</li>
</ul>
</li>
<li>Get the running count of the favourite colours overall and output this to a topic</li>
<li>A user's favourite colour can change</li>
</ul>

<p>For example:</p>
<p>Input:</p>
<ul>
<li>max,red</li>
<li>sky,blue</li>
<li>peter,green</li>
<li>max,blue (max's favourite colour updated here)</li>
<li>some bad data without comma (should be filtered out)</li>
<li>some bad data with comma,but have not colour in red or blue or green (should be filtered out)</li>
</ul>
<p>Output:</p>
<table border="1">
<tbody>
<tr>
<td>red</td>
<td>0</td>
</tr>
<tr>
<td>blue</td>
<td>2</td>
</tr>
<tr>
<td>green</td>
<td>1</td>
</tr>
</tbody>
</table>

#### Solution
<p><strong>Create 3 topics (1 optional):</strong></p>
<ul>
<li>favouritecolour-streams-input</li>
<li>favouritecolour-streams-mapped with --config cleanup.policy=compact (this topic is optional, if you want to use intermediary topic)</li>
<li>favouritecolour-streams-output with --config cleanup.policy=compact</li>
</ul>
<p><strong>Start kafka consumer</strong>: using <code>kafka-console-consumer</code></p>
<pre>kafka-console-consumer --bootstrap-server localhost:9092 \
                       --topic favouritecolour-streams-output \
                       --from-beginning \
                       --formatter kafka.tools.DefaultMessageFormatter \
                       --property print.key=true \
                       --property print.value=true \
                       --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
                       --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
</pre>
<p><strong>Start kafka streams application</strong>:</p>
<pre>kafka-run-class com.coloza.demo.kafkastreams.FavouriteColourStreamsAppRunner</pre>
<p><strong>Start kafka producer</strong>: using <code>kafka-console-producer</code> to send messages</p>
<pre>kafka-console-producer --broker-list localhost:9092 --topic favouritecolour-streams-input</pre>
<p>Now you can see what output on <code>kafka consumer</code>, keeping send messages and see output changes</p>
