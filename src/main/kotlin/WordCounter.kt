import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Produced
import java.util.*

class WordCounter {

    fun start() {
        println(".".repeat(25))
        println("WordCounter")
        println(".".repeat(5))

        println("please enter source and destination topic as source:destination")
        val input = readLine()
        if (input.isNullOrEmpty()) {
            start()
            return
        }
        val sourceTopic = input.substring(0, input.indexOf(":"))
        val sinkTopic = input.substring(input.indexOf(":") + 1)

        val props = Properties().apply {
            put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount")
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
            put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0)
            put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().javaClass.name)
            put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().javaClass.name)

            // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
            // Note: To re-run the demo, you need to use the offset reset tool:
            // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        }

        val streamBuilder = StreamsBuilder()

        val sourceData = streamBuilder.stream<String,String>(sourceTopic)


        val counts = sourceData.flatMapValues { v -> v.toLowerCase().split(" ").toList() }
            .groupBy { key, value -> value }
            .count()

        counts.mapValues { v -> v.toString() }.toStream().to(sinkTopic, Produced.with(Serdes.String(), Serdes.String()))

        val topology = streamBuilder.build()

        val streams = KafkaStreams(topology, props)
        streams.start()
        println(topology.describe())

        readLine()
    }

}


