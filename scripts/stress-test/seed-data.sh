#!/bin/bash
# Seed ~1000 records per topic with mixed payloads (100B, 1KB, 10KB)
# Runs from the host machine, producing via localhost:9092

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yaml"
NUM_TOPICS=${NUM_TOPICS:-3000}
RECORDS_PER_TOPIC=${RECORDS_PER_TOPIC:-1000}
BATCH_SIZE=20  # Topics to produce to in parallel

echo "=============================================="
echo "Stress Test - Data Seeding"
echo "=============================================="
echo "Topics: $NUM_TOPICS"
echo "Records per topic: $RECORDS_PER_TOPIC"
echo "=============================================="

# Generate payload files of different sizes
PAYLOAD_DIR=$(mktemp -d)
trap "rm -rf $PAYLOAD_DIR" EXIT

echo "Generating payload templates..."

# Small payload (~100 bytes)
python3 -c "
import string, random
payload = ''.join(random.choices(string.ascii_letters + string.digits, k=100))
print(payload)
" > "$PAYLOAD_DIR/small.txt"

# Medium payload (~1KB)
python3 -c "
import string, random
payload = ''.join(random.choices(string.ascii_letters + string.digits, k=1024))
print(payload)
" > "$PAYLOAD_DIR/medium.txt"

# Large payload (~10KB)
python3 -c "
import string, random
payload = ''.join(random.choices(string.ascii_letters + string.digits, k=10240))
print(payload)
" > "$PAYLOAD_DIR/large.txt"

echo "Payload sizes: small=$(wc -c < "$PAYLOAD_DIR/small.txt" | tr -d ' ')B, medium=$(wc -c < "$PAYLOAD_DIR/medium.txt" | tr -d ' ')B, large=$(wc -c < "$PAYLOAD_DIR/large.txt" | tr -d ' ')B"

# Pre-generate a mixed records file (reused per topic with topic name substitution)
echo "Pre-generating record template ($RECORDS_PER_TOPIC records)..."
RECORDS_FILE="$PAYLOAD_DIR/records.txt"

python3 -c "
import random
small = open('$PAYLOAD_DIR/small.txt').read().strip()
medium = open('$PAYLOAD_DIR/medium.txt').read().strip()
large = open('$PAYLOAD_DIR/large.txt').read().strip()

# Distribution: 70% small, 20% medium, 10% large
payloads = [small] * 7 + [medium] * 2 + [large] * 1

for i in range($RECORDS_PER_TOPIC):
    payload = random.choice(payloads)
    print(f'key-{i:06d}:{payload}')
" > "$RECORDS_FILE"

record_file_size=$(wc -c < "$RECORDS_FILE" | tr -d ' ')
echo "Records file size: ${record_file_size} bytes"
echo ""

# Seed data into topics using docker exec to kafka-console-producer
echo "Seeding data into $NUM_TOPICS topics..."
start_time=$(date +%s)

for ((batch_start=0; batch_start<NUM_TOPICS; batch_start+=BATCH_SIZE)); do
    batch_end=$((batch_start + BATCH_SIZE))
    if [ $batch_end -gt $NUM_TOPICS ]; then
        batch_end=$NUM_TOPICS
    fi

    for ((i=batch_start; i<batch_end; i++)); do
        topic_name=$(printf "stress-test-%04d" $i)
        docker compose -f "$COMPOSE_FILE" exec -T kafka \
            kafka-console-producer \
            --bootstrap-server kafka:29092 \
            --topic "$topic_name" \
            --property "parse.key=true" \
            --property "key.separator=:" \
            --batch-size 200 \
            < "$RECORDS_FILE" 2>/dev/null &
    done

    wait

    current=$batch_end
    elapsed=$(($(date +%s) - start_time))
    rate=0
    if [ $elapsed -gt 0 ]; then
        rate=$((current / elapsed))
    fi

    if [ $((current % 100)) -eq 0 ] || [ $current -eq $NUM_TOPICS ]; then
        echo "  Seeded topics 0-$((current-1)) ($current/$NUM_TOPICS) [${elapsed}s elapsed, ~${rate} topics/s]"
    fi
done

end_time=$(date +%s)
seed_duration=$((end_time - start_time))

total_records=$((NUM_TOPICS * RECORDS_PER_TOPIC))
echo ""
echo "=============================================="
echo "Seeding Complete!"
echo "=============================================="
echo "Topics seeded: $NUM_TOPICS"
echo "Records per topic: $RECORDS_PER_TOPIC"
echo "Total records: $total_records"
echo "Duration: ${seed_duration}s"
if [ $seed_duration -gt 0 ]; then
    echo "Rate: $((total_records / seed_duration)) records/s"
fi
echo "=============================================="
