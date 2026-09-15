# resources/kafka/dev

Place the following files here (all are gitignored):

| File | Description |
|------|-------------|
| `your-service-account.keytab` | Kerberos keytab for the dev service account |
| `krb5.conf` | KDC configuration provided by the Kafka cluster team |
| `common.pem` | SSL CA certificate for the dev Kafka cluster |

Update `app.kafka.keytab-file`, `app.kafka.krb5-conf`, and `app.kafka.ssl-ca-location`
in `application-dev.properties` to point to the actual filenames.
