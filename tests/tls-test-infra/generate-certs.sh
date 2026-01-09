#!/bin/bash
# Generate self-signed certificates for TLS testing
# Creates: CA cert, Kafka server cert, and client cert for mTLS
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CERTS_DIR="${SCRIPT_DIR}/certs"

echo "Generating TLS certificates in ${CERTS_DIR}..."

# Clean up any existing certs
rm -rf "${CERTS_DIR}"
mkdir -p "${CERTS_DIR}/ca" "${CERTS_DIR}/broker" "${CERTS_DIR}/client"

cd "${CERTS_DIR}"

# ============================================================================
# 1. Generate CA (Certificate Authority)
# ============================================================================
echo "Creating CA certificate..."

openssl genrsa -out ca/ca.key 4096

openssl req -x509 -new -nodes \
    -key ca/ca.key \
    -sha256 \
    -days 365 \
    -out ca/ca.crt \
    -subj "/CN=kafka-backup-test-ca/O=kafka-backup"

# ============================================================================
# 2. Generate Kafka broker certificate (signed by CA)
# ============================================================================
echo "Creating Kafka broker certificate..."

openssl genrsa -out broker/broker.key 2048

openssl req -new \
    -key broker/broker.key \
    -out broker/broker.csr \
    -subj "/CN=kafka/O=kafka-backup"

# Create SAN config for broker (allow localhost, kafka hostnames)
cat > broker/broker-ext.cnf << EOF
subjectAltName = @alt_names
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth, clientAuth

[alt_names]
DNS.1 = kafka
DNS.2 = kafka-tls
DNS.3 = kafka-mtls
DNS.4 = localhost
IP.1 = 127.0.0.1
EOF

openssl x509 -req \
    -in broker/broker.csr \
    -CA ca/ca.crt \
    -CAkey ca/ca.key \
    -CAcreateserial \
    -out broker/broker.crt \
    -days 365 \
    -sha256 \
    -extfile broker/broker-ext.cnf

# ============================================================================
# 3. Generate client certificate for mTLS (signed by CA)
# ============================================================================
echo "Creating client certificate for mTLS..."

openssl genrsa -out client/client.key 2048

openssl req -new \
    -key client/client.key \
    -out client/client.csr \
    -subj "/CN=kafka-backup-client/O=kafka-backup"

openssl x509 -req \
    -in client/client.csr \
    -CA ca/ca.crt \
    -CAkey ca/ca.key \
    -CAcreateserial \
    -out client/client.crt \
    -days 365 \
    -sha256

# ============================================================================
# 4. Create JKS keystores for Kafka broker (Java/Kafka requires JKS)
# ============================================================================
echo "Creating Java keystores for Kafka broker..."

# Convert broker key to PKCS#8
openssl pkcs8 -topk8 -nocrypt \
    -in broker/broker.key \
    -out broker/broker-pkcs8.key

# Create PKCS#12 bundle for broker
openssl pkcs12 -export \
    -in broker/broker.crt \
    -inkey broker/broker-pkcs8.key \
    -name kafka-broker \
    -CAfile ca/ca.crt \
    -caname kafka-ca \
    -out broker/broker.p12 \
    -password pass:changeit

# Import PKCS#12 into JKS keystore
keytool -importkeystore \
    -deststorepass changeit \
    -destkeypass changeit \
    -destkeystore broker/kafka.keystore.jks \
    -srckeystore broker/broker.p12 \
    -srcstoretype PKCS12 \
    -srcstorepass changeit \
    -alias kafka-broker \
    -noprompt 2>/dev/null || true

# Create truststore with CA cert
keytool -importcert \
    -keystore broker/kafka.truststore.jks \
    -storepass changeit \
    -alias kafka-ca \
    -file ca/ca.crt \
    -noprompt 2>/dev/null || true

# ============================================================================
# 5. Create client keystore/truststore (for Java clients testing)
# ============================================================================
echo "Creating client keystores..."

# Convert client key to PKCS#8
openssl pkcs8 -topk8 -nocrypt \
    -in client/client.key \
    -out client/client-pkcs8.key

# Create PKCS#12 bundle for client
openssl pkcs12 -export \
    -in client/client.crt \
    -inkey client/client-pkcs8.key \
    -name kafka-client \
    -CAfile ca/ca.crt \
    -caname kafka-ca \
    -out client/client.p12 \
    -password pass:changeit

# Client keystore
keytool -importkeystore \
    -deststorepass changeit \
    -destkeypass changeit \
    -destkeystore client/client.keystore.jks \
    -srckeystore client/client.p12 \
    -srcstoretype PKCS12 \
    -srcstorepass changeit \
    -alias kafka-client \
    -noprompt 2>/dev/null || true

# Client truststore with CA
keytool -importcert \
    -keystore client/client.truststore.jks \
    -storepass changeit \
    -alias kafka-ca \
    -file ca/ca.crt \
    -noprompt 2>/dev/null || true

# ============================================================================
# Summary
# ============================================================================
echo ""
echo "=============================================="
echo "TLS certificates generated successfully!"
echo "=============================================="
echo ""
echo "PEM files for kafka-backup (Rust client):"
echo "  CA cert:     ${CERTS_DIR}/ca/ca.crt"
echo "  Client cert: ${CERTS_DIR}/client/client.crt"
echo "  Client key:  ${CERTS_DIR}/client/client.key"
echo ""
echo "JKS files for Kafka broker:"
echo "  Keystore:    ${CERTS_DIR}/broker/kafka.keystore.jks"
echo "  Truststore:  ${CERTS_DIR}/broker/kafka.truststore.jks"
echo "  Password:    changeit"
echo ""
echo "Example kafka-backup config:"
echo "  security:"
echo "    security_protocol: SSL"
echo "    ssl_ca_location: ${CERTS_DIR}/ca/ca.crt"
echo "    ssl_certificate_location: ${CERTS_DIR}/client/client.crt"
echo "    ssl_key_location: ${CERTS_DIR}/client/client.key"
echo ""
