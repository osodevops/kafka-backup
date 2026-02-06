#!/bin/bash
# Create 3000 topics for stress testing
# Runs inside the kafka-setup Docker container

set -e

KAFKA_BOOTSTRAP_SERVER=${KAFKA_BOOTSTRAP_SERVER:-kafka:29092}
NUM_TOPICS=${NUM_TOPICS:-3000}
BATCH_SIZE=50

echo "=============================================="
echo "Stress Test - Topic Creation"
echo "=============================================="
echo "Kafka Bootstrap: $KAFKA_BOOTSTRAP_SERVER"
echo "Number of topics: $NUM_TOPICS"
echo "=============================================="

# Wait for Kafka to be fully ready
echo "Waiting for Kafka to be ready..."
sleep 5

for i in {1..30}; do
    if kafka-topics --bootstrap-server $KAFKA_BOOTSTRAP_SERVER --list > /dev/null 2>&1; then
        echo "Kafka is ready!"
        break
    fi
    echo "  Waiting for Kafka... ($i/30)"
    sleep 2
done

echo ""
echo "Creating $NUM_TOPICS topics in parallel batches of $BATCH_SIZE..."
start_time=$(date +%s)

for ((batch_start=0; batch_start<NUM_TOPICS; batch_start+=BATCH_SIZE)); do
    batch_end=$((batch_start + BATCH_SIZE))
    if [ $batch_end -gt $NUM_TOPICS ]; then
        batch_end=$NUM_TOPICS
    fi

    # Create this batch in parallel (background jobs)
    for ((i=batch_start; i<batch_end; i++)); do
        topic_name=$(printf "stress-test-%04d" $i)
        kafka-topics --bootstrap-server $KAFKA_BOOTSTRAP_SERVER \
            --create --topic "$topic_name" \
            --partitions 1 \
            --replication-factor 1 \
            --if-not-exists > /dev/null 2>&1 &
    done

    # Wait for batch to complete
    wait

    if [ $((batch_end % 500)) -eq 0 ] || [ $batch_end -eq $NUM_TOPICS ]; then
        echo "Created topics 0 to $((batch_end-1)) ($batch_end/$NUM_TOPICS)"
    fi
done

end_time=$(date +%s)
topic_creation_duration=$((end_time - start_time))
echo "Topic creation completed in ${topic_creation_duration}s"

# Verify topic count
sleep 2
actual_count=$(kafka-topics --bootstrap-server $KAFKA_BOOTSTRAP_SERVER --list 2>/dev/null | grep -c "stress-test-" || echo "0")
echo ""
echo "=============================================="
echo "Setup Complete!"
echo "=============================================="
echo "Topics created: $actual_count"
echo "Topic creation time: ${topic_creation_duration}s"
echo "=============================================="
