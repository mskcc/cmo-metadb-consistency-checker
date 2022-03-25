# SMILE Consistency Checker 🔍

The Consistency Checker is one of several components that comprise the SMILE distributed microservices system. Its role is to ensure that there is no data loss by comparing the message contents for a given LIMS Request ID directly from the LIMSRest service with the corresponding message contents from the SMILE service.

## Run

### Custom properties

Make an `application.properties` based on [application.properties.EXAMPLE](src/main/resources/application.properties.EXAMPLE). 

All properties are required with the exception of some NATS connection-specific properties. The following are only required if `nats.tls_channel` is set to `true`:

- `nats.keystore_path` : path to client keystore
- `nats.truststore_path` : path to client truststore
- `nats.key_password` : keystore password
- `nats.store_password` : truststore password

### Locally

**Requirements:**
- maven 3.6.1
- java 8

Add `application.properties` and `log4j.properties` to the local application resources: `src/main/resources`

Build with 

```
mvn clean install
```

Run with 

```
java -jar target/consistency_checker.jar
```

### With Docker

**Requirements**
- docker

Build image with Docker

```
docker build -t <repo>/<tag>:<version> .
```

Push image to DockerHub 

```
docker push <repo>/<tag>:<version>
```

If the Docker image is built with the properties baked in then simply run with:


```
docker run --name consistency-checker <repo>/<tag>:<version> \
	-jar /consistency-checker/consistency_checker.jar
```

Otherwise use a bind mount to make the local files available to the Docker image and add  `--spring.config.location` to the java arg

```
docker run --mount type=bind,source=<local path to properties files>,target=/consistency-checker/src/main/resources \
	--name consistency-checker <repo>/<tag>:<version> \
	-jar /consistency-checker/consistency_checker.jar \
	--spring.config.location=/consistency-checker/src/main/resources/application.properties
```
