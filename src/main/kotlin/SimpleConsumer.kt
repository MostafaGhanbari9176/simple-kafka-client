import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import java.time.Duration
import java.util.*

class SimpleConsumer {
    private var topic: String? = ""

    fun start() {
        println("-".repeat(25))
        println("simple consumer")
        println("-".repeat(5))

        println("inter topic name")

        topic = readLine()
        if (topic.isNullOrEmpty()) {
            start()
            return
        }

        val props = Properties().apply {
            put("bootstrap.servers", "127.0.0.1:9094")
            put("group.id", "test")
            put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
            put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        }

        val consumer = KafkaConsumer<String, String>(props)

        printData(consumer)
    }

    private fun printData(consumer: KafkaConsumer<String, String>) {
        consumer.subscribe(listOf(topic), object : ConsumerRebalanceListener {
            override fun onPartitionsRevoked(partitions: MutableCollection<TopicPartition>?) {
                println("partition revoked:${partitions?.joinToString(", ") { p -> "${p.topic()}:${p.partition()}" }}")
            }

            override fun onPartitionsAssigned(partitions: MutableCollection<TopicPartition>?) {
                println("partition assigned:${partitions?.joinToString(", ") { p -> "${p.topic()}:${p.partition()}" }}")
            }
        })

        while (true) {
            val records = consumer.poll(Duration.ofMillis(100))

            records.forEach { r ->
                println("k:${r.key()}, v:${r.value()}, t:${r.topic()}, p:${r.partition()}, o:${r.offset()}")
            }
        }
    }
}

