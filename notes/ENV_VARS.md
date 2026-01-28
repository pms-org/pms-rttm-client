# Kafka Common - Environment Variables

This document lists all the environment variables that can be used to override configuration in `application.yml`.

## Configuration Flow

```
Environment Variables → application.yml → PmsKafkaConfig.java
```

1. **Environment Variables**: Override default values
2. **application.yml**: Contains properties with ENV var placeholders
3. **PmsKafkaConfig.java**: Reads from Spring Boot properties (`spring.kafka.*`)

## Available Environment Variables

All environment variables are optional. If not set, the default values from `application.yml` will be used:

| Variable Name | Default Value | Description | application.yml Property |
|--------------|---------------|-------------|--------------------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka bootstrap servers (comma-separated list) | `spring.kafka.bootstrap-servers` |
| `KAFKA_PRODUCER_ACKS` | `all` | Producer acknowledgment mode | `spring.kafka.producer.acks` |
| `KAFKA_PRODUCER_RETRIES` | `3` | Number of retries for failed producer sends | `spring.kafka.producer.retries` |
| `KAFKA_CONSUMER_GROUP_ID` | `pms-consumer-group` | Consumer group ID | `spring.kafka.consumer.group-id` |
| `KAFKA_CONSUMER_AUTO_OFFSET_RESET` | `earliest` | What to do when there is no initial offset | `spring.kafka.consumer.auto-offset-reset` |
| `KAFKA_CONSUMER_ENABLE_AUTO_COMMIT` | `false` | Whether to enable auto-commit | `spring.kafka.consumer.enable-auto-commit` |
| `KAFKA_LISTENER_ACK_MODE` | `manual_immediate` | Acknowledgment mode for Kafka listeners | `spring.kafka.listener.ack-mode` |

## Usage Examples

### Setting Environment Variables

#### Windows (CMD)
```cmd
set KAFKA_BOOTSTRAP_SERVERS=localhost:9092
set KAFKA_PRODUCER_ACKS=all
set KAFKA_PRODUCER_RETRIES=3
set KAFKA_CONSUMER_GROUP_ID=pms-consumer-group
set KAFKA_CONSUMER_AUTO_OFFSET_RESET=earliest
set KAFKA_CONSUMER_ENABLE_AUTO_COMMIT=false
set KAFKA_LISTENER_ACK_MODE=manual_immediate
```

#### Windows (PowerShell)
```powershell
$env:KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
$env:KAFKA_PRODUCER_ACKS="all"
$env:KAFKA_PRODUCER_RETRIES="3"
$env:KAFKA_CONSUMER_GROUP_ID="pms-consumer-group"
$env:KAFKA_CONSUMER_AUTO_OFFSET_RESET="earliest"
$env:KAFKA_CONSUMER_ENABLE_AUTO_COMMIT="false"
$env:KAFKA_LISTENER_ACK_MODE="manual_immediate"
```

#### Linux/Mac (Bash)
```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export KAFKA_PRODUCER_ACKS=all
export KAFKA_PRODUCER_RETRIES=3
export KAFKA_CONSUMER_GROUP_ID=pms-consumer-group
export KAFKA_CONSUMER_AUTO_OFFSET_RESET=earliest
export KAFKA_CONSUMER_ENABLE_AUTO_COMMIT=false
export KAFKA_LISTENER_ACK_MODE=manual_immediate
```

### Using .env file

If you're using Spring Boot with a `.env` file loader (like `spring-dotenv`), create a `.env` file:

```properties
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_PRODUCER_ACKS=all
KAFKA_PRODUCER_RETRIES=3
KAFKA_CONSUMER_GROUP_ID=pms-consumer-group
KAFKA_CONSUMER_AUTO_OFFSET_RESET=earliest
KAFKA_CONSUMER_ENABLE_AUTO_COMMIT=false
KAFKA_LISTENER_ACK_MODE=manual_immediate
```

### Using Spring Profiles

You can also create profile-specific YAML files:

**application-dev.yml**
```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: pms-dev-group
```

**application-prod.yml**
```yaml
spring:
  kafka:
    bootstrap-servers: prod-kafka-1:9092,prod-kafka-2:9092
    consumer:
      group-id: pms-prod-group
```

Then run with:
```bash
java -jar kafka-common.jar --spring.profiles.active=prod
```

## Valid Values

### KAFKA_PRODUCER_ACKS
- `0` - Fire and forget (no acknowledgment)
- `1` - Leader acknowledgment
- `all` or `-1` - All in-sync replicas acknowledgment (default, most reliable)

### KAFKA_CONSUMER_AUTO_OFFSET_RESET
- `earliest` - Start from the beginning (default)
- `latest` - Start from the latest message
- `none` - Throw exception if no offset found

### KAFKA_LISTENER_ACK_MODE
- `record` - Commit after each record is processed
- `batch` - Commit after each batch of records
- `time` - Commit at a fixed time interval
- `count` - Commit after a fixed number of records
- `count_time` - Combination of COUNT and TIME
- `manual` - Manual commit via Acknowledgment
- `manual_immediate` - Manual immediate commit (default)

## How It Works

Spring Boot's property resolution follows this order (highest to lowest priority):

1. **Command-line arguments**: `--spring.kafka.bootstrap-servers=...`
2. **Environment variables**: `KAFKA_BOOTSTRAP_SERVERS=...`
3. **application-{profile}.yml**: Profile-specific configurations
4. **application.yml**: Default configurations with placeholders like `${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}`

This allows for flexible configuration across different environments without code changes!
