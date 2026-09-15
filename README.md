# DAMS Message Parsing — Java / Spring Boot

Spring Boot batch job that reads XML messages from a Kafka topic, converts them to JSON,
and updates an Oracle DB record matched by `PAReferenceId`.

---

## How it differs from the Python version

| Aspect | Python | Java (this repo) |
|--------|--------|-----------------|
| Kerberos | Needs external `kinit` call | JAAS `Krb5LoginModule` handles ticket acquisition directly from keytab — no `kinit` needed |
| Config | `envs/.env.<env>` files | `application-<env>.properties` + Spring profiles |
| Run | `python main.py` | `java -jar target/message-parsing-1.0.0.jar` |
| GSSAPI | Requires librdkafka compiled with GSSAPI | Built in to JVM — no extra system libraries needed |

---

## Prerequisites

- Java 17+
- Maven 3.8+
- Oracle JDBC accessible via Maven Central (included in `pom.xml`)

---

## Setup

**1. Copy and fill in the properties file for your environment:**

```bash
cp src/main/resources/application-dev.properties.example \
   src/main/resources/application-dev.properties
# Edit application-dev.properties with real broker/Oracle values
```

**2. Place the Kerberos/SSL files (gitignored):**

```
resources/kafka/dev/
  ├── your-service-account.keytab
  ├── krb5.conf
  └── common.pem
```

**3. Build:**

```bash
mvn clean package -DskipTests
```

---

## Running

```bash
# Dev (default)
java -jar target/message-parsing-1.0.0.jar

# QA
java -Dspring.profiles.active=qa -jar target/message-parsing-1.0.0.jar

# Prod
java -Dspring.profiles.active=prod -jar target/message-parsing-1.0.0.jar
```

Or set the environment variable:

```bash
export SPRING_PROFILES_ACTIVE=prod
java -jar target/message-parsing-1.0.0.jar
```

---

## Cron example

```cron
0 6 * * * export SPRING_PROFILES_ACTIVE=prod && \
  java -jar /opt/apps/kafka/DAMS-Message-Parsing-Java/target/message-parsing-1.0.0.jar \
  >> /opt/apps/kafka/logs/dams-parsing.log 2>&1
```

---

## Project structure

```
src/main/java/com/dams/messageparsing/
├── DamsMessageParsingApplication.java   # Spring Boot entry point
├── batch/
│   └── BatchRunner.java                 # CommandLineRunner — polls and exits
├── consumer/
│   └── KafkaMessageConsumer.java        # Raw KafkaConsumer wrapper
├── db/
│   └── OracleHandler.java               # JdbcTemplate-based Oracle access
├── parser/
│   ├── ParseResult.java                 # (record) holds parsed data + JSON string
│   └── XmlToJsonParser.java             # XML → Map → compact JSON
└── processor/
    ├── MessageProcessor.java            # Full pipeline per message
    └── PermanentMessageException.java   # Signals non-retryable failures
```
