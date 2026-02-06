#!/bin/bash
# Start/stop continuous background producers targeting ~100 topics at ~500 rec/sec total
# Usage: ./run-continuous-producers.sh start|stop
#
# Producers run inside the Kafka container as detached processes.
# Stop kills them via pkill inside the container.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yaml"
NUM_PRODUCER_TOPICS=${NUM_PRODUCER_TOPICS:-100}
RECORDS_PER_SEC_PER_TOPIC=${RECORDS_PER_SEC_PER_TOPIC:-5}
NUM_TOPICS=${NUM_TOPICS:-3000}

start_producers() {
    echo "Starting continuous producers..."
    echo "  Target topics: $NUM_PRODUCER_TOPICS (out of $NUM_TOPICS)"
    echo "  Rate per topic: $RECORDS_PER_SEC_PER_TOPIC rec/sec"
    echo "  Total target rate: $((NUM_PRODUCER_TOPICS * RECORDS_PER_SEC_PER_TOPIC)) rec/sec"
    echo ""

    # Select evenly-spaced topics from the full topic range
    step=$((NUM_TOPICS / NUM_PRODUCER_TOPICS))

    started=0
    for ((idx=0; idx<NUM_PRODUCER_TOPICS; idx++)); do
        topic_num=$((idx * step))
        topic_name=$(printf "stress-test-%04d" $topic_num)

        # Launch kafka-producer-perf-test detached inside the container
        # --num-records very high so it runs until killed
        # --throughput controls rate per topic
        docker compose -f "$COMPOSE_FILE" exec -T -d kafka \
            kafka-producer-perf-test \
            --topic "$topic_name" \
            --num-records 999999 \
            --throughput $RECORDS_PER_SEC_PER_TOPIC \
            --record-size 500 \
            --producer-props bootstrap.servers=kafka:29092 \
            > /dev/null 2>&1

        started=$((started + 1))

        if [ $((started % 20)) -eq 0 ]; then
            echo "  Started $started/$NUM_PRODUCER_TOPICS producers"
        fi
    done

    echo ""
    echo "Started $started continuous producers (detached in Kafka container)"
    echo "Total target throughput: ~$((NUM_PRODUCER_TOPICS * RECORDS_PER_SEC_PER_TOPIC)) records/sec"
}

stop_producers() {
    echo "Stopping continuous producers..."

    # Kill all kafka-producer-perf-test processes inside the container
    docker compose -f "$COMPOSE_FILE" exec -T kafka \
        bash -c 'pkill -f kafka-producer-perf-test 2>/dev/null; echo "killed $(pgrep -cf kafka-producer-perf-test 2>/dev/null || echo 0) remaining"' 2>/dev/null || true

    echo "Producers stopped"
}

case "${1:-}" in
    start)
        start_producers
        ;;
    stop)
        stop_producers
        ;;
    *)
        echo "Usage: $0 start|stop"
        echo ""
        echo "Environment variables:"
        echo "  NUM_PRODUCER_TOPICS  - Number of topics to produce to (default: 100)"
        echo "  RECORDS_PER_SEC_PER_TOPIC - Records per second per topic (default: 5)"
        echo "  NUM_TOPICS           - Total topic count for index spacing (default: 3000)"
        exit 1
        ;;
esac
