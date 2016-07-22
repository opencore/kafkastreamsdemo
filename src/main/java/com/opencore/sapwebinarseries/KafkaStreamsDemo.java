/**
 * Copyright 2016 OpenCore GmbH & Co. KG
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.opencore.sapwebinarseries;

import com.opencore.sapwebinarseries.utils.GenericAvroSerde;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;

/**
 * This code was used for the demo during a Opencore webinar on Kafka held in cooperation with SAP.
 *
 * It shows a simple use case of Kafka Streams based on twitter data. Tweets are taken from the "twitter"
 * topic in Kafka. The Kafka Connect job to write them into this topic can be found at:
 * https://github.com/Eneco/kafka-connect-twitter
 *
 * Tweets are then split into individual words and these are counted, with the counts being stored in
 * different topics:
 *  - wordcount:    the raw overall count
 *  - wordcount5m:  a tumbling window of five minutes - only words that have 3 or more hits are displayed
 *  - buzzwords:    again, a 5 minute window count, but this stream is filtered to only buzzwords that are of interest
 *
 */
public class KafkaStreamsDemo {
  private static Logger logger;
  private static String kafkaIPAdress = "127.0.0.1";

  public static void main(String[] args) {
    logger = LoggerFactory.getLogger(KafkaStreamsDemo.class.getName());

    // Check if a Kafka ip was defined via environment variable "KAFKAIP"
    if (System.getenv().containsKey("KAFKAIP")) {
      kafkaIPAdress = System.getenv("KAFKAIP");
    }

    // Load wordlists from files - stopwords will be ignored during processing, buzzwords will be filtered into an
    // extra buzzword topic
    ArrayList<String> buzzWords = loadWordList("buzzwords.txt");
    ArrayList<String> stopWords = loadWordList("stopwords.txt");

    Properties streamsConfiguration = new Properties();

    // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
    // against which the application is run.
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "streamsdemo");

    // Where to find Kafka broker(s).
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaIPAdress + ":9092");

    // Where to find the corresponding ZooKeeper ensemble.
    streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, kafkaIPAdress + ":2181");

    // Where to find the Confluent schema registry instance(s)
    streamsConfiguration.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://" + kafkaIPAdress + ":8081");

    // Specify default (de)serializers for record keys and for record values.
    streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);

    // Initialize specific serializers to be used later
    final Serde<String> string_serde = Serdes.String();
    final Serde<Long> long_serde = Serdes.Long();

    KStreamBuilder builder = new KStreamBuilder();

    // read the source stream - "twitter" - this must contain twitter records in avro format
    // as provided by https://github.com/Eneco/kafka-connect-twitter
    KStream<String, GenericRecord> tweetFeed = builder.stream("twitter");

    KStream<String, String> splitTweets = tweetFeed.map(new KeyValueMapper<String, GenericRecord, KeyValue<String, String>>() {
      @Override
      public KeyValue<String, String> apply(String key, GenericRecord value) {
        return new KeyValue<>(value.get("id").toString(), value.get("text").toString());
      }
    })
        // Split the tweets text at whitespace characters
        // (we also replace "big data" with "bigdata" here, as that is a relevant word for us and would otherwise not be treated as one term
        .flatMapValues(value -> Arrays.asList(value.toLowerCase().replace("big data", "bigdata").split("\\s+")))
        // Map the value (individual word) as key, since we want to use .countByKey later on
        .map((key, word) -> new KeyValue<>(word, word))
        // filter out stopwords
        .filter((key, count) -> !stopWords.contains(key))
        // remove empty records
        .filter(((key, value) -> value != null));

    // Count the individual words and output the running tally to the "wordcount" topic
    splitTweets.countByKey("count").mapValues(value -> value.toString())
        .to(string_serde, string_serde, "wordcount");

    // Count individual words as well, but this time for a 5 minute tumbling window
    splitTweets.countByKey(TimeWindows.of("WordCountWindow", 5 * 60 * 1000L)).mapValues(value -> value.toString())
        .toStream()
        // only output counts that are larger than 3 to cut down on noise
        .filter((windowedUserId, count) -> Integer.valueOf(count) >= 3)
        // perform a bit of formating on the key - to get a human readable datetime and the counted word
        .map((windowedUserId, count) -> new KeyValue<>(new Date(windowedUserId.window().end()).toString() + "-" + windowedUserId.key(), count))
        // output records to a topic for consumption
        .to(string_serde, string_serde, "wordcount5m");

    // Filter buzzwords that are of special interest and count them in an extra topic
    // we do not use the wordcount5m topic as input here, since that only displays occurrences larger than 3, but for
    // buzzwords we are interested in even single occurrences
    splitTweets.countByKey(TimeWindows.of("BuzzWordCountWindow", 5 * 60 * 1000L)).mapValues(value -> value.toString())
        .toStream()
        // filter stream to only buzzwords or hashtags of that buzzword
        .filter((windowedUserId, count) -> (buzzWords.contains(windowedUserId.key()) || buzzWords.contains("#" + windowedUserId.key())))
        // perform a bit of formating on the key - to get a human readable datetime and the counted word
        .map((windowedUserId, count) -> new KeyValue<>(new Date(windowedUserId.window().end()).toString() + "-" + windowedUserId.key(), count))
        // output stream to Kafka topic
        .to(string_serde, string_serde, "buzzwords");

    // Start processing the rules above
    KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
    streams.start();
  }

  /**
   * Read textfiles into an ArrayList<String>
   *
   * @param fileName The name of the file to load
   * @return an ArrayList of Strings, each element of the List is one line of the file
   */
  private static ArrayList<String> loadWordList(String fileName) {
    ArrayList<String> result = new ArrayList<>();
    BufferedReader fileReader = new BufferedReader(new InputStreamReader(KafkaStreamsDemo.class.getClassLoader().getResourceAsStream(fileName)));
    String currentLine;
    try {
      while ((currentLine = fileReader.readLine()) != null) {
        result.add(currentLine);
      }
    } catch (IOException e) {
      logger.warn("Exception caught while loading wordlist from " + fileName + " - proceeding with potentially incomplete list..");
      return result;
    }
    return result;
  }
}
