#!/bin/bash
# Create topics and populate with data for Issue #29 regression testing
# Optimized for faster topic creation

set -e

KAFKA_BOOTSTRAP_SERVER=${KAFKA_BOOTSTRAP_SERVER:-kafka:29092}
NUM_TOPICS=${NUM_TOPICS:-3000}
RECORDS_PER_TOPIC=${RECORDS_PER_TOPIC:-10}
BATCH_SIZE=50

echo "=============================================="
echo "Issue #29 Verification - Topic Setup"
echo "=============================================="
echo "Kafka Bootstrap: $KAFKA_BOOTSTRAP_SERVER"
echo "Number of topics: $NUM_TOPICS"
echo "Records per topic: $RECORDS_PER_TOPIC"
echo "=============================================="

# Wait for Kafka to be fully ready
echo "Waiting for Kafka to be ready..."
sleep 5

# Verify connection
for i in {1..30}; do
    if kafka-topics --bootstrap-server $KAFKA_BOOTSTRAP_SERVER --list > /dev/null 2>&1; then
        echo "Kafka is ready!"
        break
    fi
    echo "  Waiting for Kafka... ($i/30)"
    sleep 2
done

echo ""
echo "Creating $NUM_TOPICS topics in parallel batches..."
start_time=$(date +%s)

# Create topics in parallel batches using background processes
for ((batch_start=0; batch_start<NUM_TOPICS; batch_start+=BATCH_SIZE)); do
    batch_end=$((batch_start + BATCH_SIZE))
    if [ $batch_end -gt $NUM_TOPICS ]; then
        batch_end=$NUM_TOPICS
    fi

    # Create this batch in parallel (background jobs)
    for ((i=batch_start; i<batch_end; i++)); do
        topic_name=$(printf "test-topic-%05d" $i)
        kafka-topics --bootstrap-server $KAFKA_BOOTSTRAP_SERVER \
            --create --topic "$topic_name" \
            --partitions 1 \
            --replication-factor 1 \
            --if-not-exists > /dev/null 2>&1 &
    done

    # Wait for batch to complete
    wait

    echo "Created topics $batch_start to $((batch_end-1)) ($batch_end/$NUM_TOPICS)"
done

end_time=$(date +%s)
topic_creation_duration=$((end_time - start_time))
echo "Topic creation completed in ${topic_creation_duration}s"

# Verify topic count
sleep 2
actual_count=$(kafka-topics --bootstrap-server $KAFKA_BOOTSTRAP_SERVER --list 2>/dev/null | wc -l | tr -d ' ')
echo "Verified: $actual_count topics exist"

# Produce minimal data to each topic (faster - just 1 record each for testing)
echo ""
echo "Producing $RECORDS_PER_TOPIC record(s) to each topic..."
start_time=$(date +%s)

# Use parallel production as well
for ((batch_start=0; batch_start<NUM_TOPICS; batch_start+=BATCH_SIZE)); do
    batch_end=$((batch_start + BATCH_SIZE))
    if [ $batch_end -gt $NUM_TOPICS ]; then
        batch_end=$NUM_TOPICS
    fi

    for ((i=batch_start; i<batch_end; i++)); do
        topic_name=$(printf "test-topic-%05d" $i)
        # Generate records
        (
            for ((r=0; r<RECORDS_PER_TOPIC; r++)); do
                echo "key-$r:value-$r-for-$topic_name"
            done | kafka-console-producer \
                --bootstrap-server $KAFKA_BOOTSTRAP_SERVER \
                --topic "$topic_name" \
                --property "parse.key=true" \
                --property "key.separator=:" 2>/dev/null
        ) &
    done

    wait

    if [ $((batch_end % 500)) -eq 0 ] || [ $batch_end -eq $NUM_TOPICS ]; then
        echo "Produced to topics 0-$((batch_end-1))"
    fi
done

end_time=$(date +%s)
produce_duration=$((end_time - start_time))
echo "Data production completed in ${produce_duration}s"

# Summary
total_records=$((NUM_TOPICS * RECORDS_PER_TOPIC))
echo ""
echo "=============================================="
echo "Setup Complete!"
echo "=============================================="
echo "Topics created: $actual_count"
echo "Records per topic: $RECORDS_PER_TOPIC"
echo "Total records: $total_records"
echo "Topic creation time: ${topic_creation_duration}s"
echo "Data production time: ${produce_duration}s"
echo ""
echo "Ready for backup testing!"
echo "=============================================="
