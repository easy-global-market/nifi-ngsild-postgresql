# NiFi NGSI-LD to PostgreSQL Processor

[![License: Apache-2.0](https://img.shields.io/github/license/easy-global-market/nifi-ngsild-postgresql.svg)](https://spdx.org/licenses/Apache-2.0.html)
<br>
![Build](https://github.com/easy-global-market/nifi-ngsild-postgresql/actions/workflows/maven.yml/badge.svg)
[![CodeQL](https://github.com/easy-global-market/nifi-ngsild-postgresql/actions/workflows/codeql.yml/badge.svg)](https://github.com/stellio-hub/stellio-context-broker/actions/workflows/codeql.yml)

## Presentation

The NiFi NGSI-LD to PostgreSQL Processor is an Apache NiFi processor that provides seamless integration between
NGSI-LD Context Brokers and PostgreSQL databases. This processor automatically transforms NGSI-LD data into
structured relational database tables, enabling real-time data warehousing and analytics capabilities for FIWARE ecosystems.

Key features include:
- **Automatic Database Schema Generation**: Dynamically creates PostgreSQL tables based on NGSI-LD entities types, eliminating manual database design
- **NGSI-LD Standard Compliance**: Full support for NGSI-LD temporal data, multi-attributes, relationships, and system attributes (createdAt, modifiedAt, observedAt)
- **Flexible Data Transformation**: Configurable flattening of observations, attribute filtering, customizable table naming conventions, ...
- **Enterprise-Grade Processing**: Batch processing support, connection pooling, error handling, and retry mechanisms for production environments
- **Data denormalization**: Leverage NGSI-LD Linked Entity Retrieval to export data from first-level relationships into the same table as the main entity

The processor allows organizations to build comprehensive data pipelines that connect FIWARE-enabled IoT devices and 
applications directly to their analytical infrastructure.

## Why use it?

Modern smart city and IoT deployments generate massive volumes of data that need to be stored, analyzed, and
visualized for actionable insights. Traditional approaches require complex custom integrations between FIWARE components
and database systems, leading to development overhead and maintenance challenges.

This processor eliminates the integration complexity by providing a production-ready, standards-compliant solution
that bridges the gap between NGSI-LD Context Brokers and enterprise data analytics platforms.
Organizations can immediately start analyzing their data without investing in custom development or dealing with
format conversion complexities.

## Developer guidelines

### Overview

This section provides developers with the information needed to understand, build, modify, and contribute to the project.

### Prerequisites

- Java Development Kit (JDK) 21 or higher
- Apache Maven 3.6.0 or higher
- Docker (for running integration tests with TestContainers)
- PostgreSQL 16 or higher, with PostGIS extension (for local development and testing)
- Git for version control

### Project Structure

```
nifi-ngsild-postgresql/
├── pom.xml                                    # Parent Maven POM
├── nifi-ngsild-postgresql-processors/         # Core processor implementation
│   ├── src/main/java/egm/io/nifi/processors/ngsild/
│   │   ├── NgsiLdToPostgreSQL.java           # Main processor class
│   │   ├── PostgreSQLTransformer.java        # Data transformation logic
│   │   ├── model/                            # Data model classes
│   │   └── utils/                            # Utility classes
│   ├── src/test/java/                        # Unit and integration tests
│   └── src/test/resources/                   # Test data files
└── nifi-ngsild-postgresql-nar/               # NiFi Archive (NAR) packaging
```

### Building the Project

1. Clone the repository

   ```bash
   git clone https://github.com/easy-global-market/nifi-ngsild-postgresql
   cd nifi-ngsild-postgresql
   ```

2. Build the project

   ```bash
   mvn clean compile
   ```

3. Run tests

   ```bash
   mvn test
   ```

   Note: Integration tests require Docker to be running as they use TestContainers for PostgreSQL.

4. Package the NAR file

   ```bash
   mvn clean package
   ```

   This creates the deployable NAR file in `nifi-ngsild-postgresql-nar/target/`.

### Running Tests

The project includes comprehensive unit and integration tests:

- Unit Tests: Test individual components and logic
- Integration Tests: Use TestContainers to spin up PostgreSQL instances for realistic testing

Test execution:

   ```bash
   # Run all tests
   mvn test
   
   # Run specific test class
   mvn test -Dtest=TestNgsiLdToPostgreSQL
   
   # Skip integration tests (if Docker is not available)
   mvn test -DskipITs=true
   ```

Test Configuration:
- Tests use `postgis/postgis:16-3.5-alpine` Docker image
- Automatic database cleanup after each test
- Mock NiFi environment for processor testing

## Installation Guidelines

### Requirements

- Apache NiFi: Version 2.4.0 or higher
  - With PostgreSQL JDBC driver version 42.7.6 or higher
- PostgreSQL Database: Version 16 or higher, with PostGIS extension installed and enabled
  - Can be local or remote. Ensure network connectivity from NiFi instance.
- Follow [NiFi System Administrator's Guide](https://nifi.apache.org/nifi-docs/administration-guide.html) for other system requirements and recommendations

### Installation Methods

#### Method 1: Pre-built NAR Installation

1. Download the NAR file
   ```bash
   wget https://github.com/easy-global-market/nifi-ngsild-postgresql/releases/download/2.4.0/nifi-ngsild-postgresql-nar-2.4.0.nar
   ```

2. Copy NAR and JDBC driver to NiFi
   ```bash
   cp nifi-ngsild-postgresql-nar-2.4.0.nar $NIFI_HOME/lib/
   cp postgresql-42.7.6.jar $NIFI_HOME/lib/
   ```

3. Restart NiFi
   ```bash
   $NIFI_HOME/bin/nifi.sh restart
   ```

#### Method 2: Build from Source

1. See the [Developer Guidelines](#developer-guidelines) section for software requirements and project setup

2. Build the NAR file
   ```bash
   mvn clean package
   ```

3. Install the built NAR
   ```bash
   cp nifi-ngsild-postgresql-nar/target/nifi-ngsild-postgresql-nar-2.4.0.nar $NIFI_HOME/lib/
   ```

4. Restart NiFi
   ```bash
   $NIFI_HOME/bin/nifi.sh restart
   ```

## NiFi Configuration

### Database Connection Pool Setup

1. Access NiFi UI
    - Navigate to https://localhost:8443/nifi
    - Login with NiFi credentials

2. Create DBCPConnectionPool Controller Service
    - Go to Controller Services
    - Add Controller Service → DBCPConnectionPool
    - Configure properties:

   | Property                   | Value                                | Example                                    |
   |----------------------------|--------------------------------------|--------------------------------------------|
   | Database Connection URL    | jdbc:postgresql://host:port/database | jdbc:postgresql://localhost:5432/nifi_data |
   | Database Driver Class Name | org.postgresql.Driver                | org.postgresql.Driver                      |
   | Database User              | nifi_user                            | nifi_user                                  |
   | Password                   | secure_password                      | ***********                                |
   | Max Wait Time              | 500 millis                           | 500 millis                                 |
   | Max Total Connections      | 8                                    | 8                                          |

3. Enable the service
    - Click enable
    - Verify service shows "Enabled" status

### Processor Configuration

1. Configure Required Properties

   | Property             | Description                     | Default    |
   |----------------------|---------------------------------|------------|
   | JDBC Connection Pool | Reference to DBCPConnectionPool | (required) |
   | Batch Size           | FlowFiles per transaction       | 10         |

2. Configure Optional Properties

   | Property                      | Description                      | Default              |
   |-------------------------------|----------------------------------|----------------------|
   | DB Schema                     | Target database schema           | stellio              |
   | Table Name Suffix             | Suffix for table names           | (none)               |
   | Flatten Observations          | Generic observation columns      | false                |
   | Ignore Empty Observed At      | Skip records without timestamps  | true                 |
   | Dataset id prefix to truncate | Truncate prefix from dataset ids | urn:ngsi-ld:Dataset: |
   | Export System Attributes      | Include createdAt, modifiedAt    | false                |
   | Ignored Attributes            | Comma-separated attribute list   | (none)               |

### Sample Flows

To help you get started, the project includes a [sample process group](./samples/exporter_process_group_definition.json)
that demonstrates the processor's capabilities. It contains three process groups:
- A process group that receives notifications and exports data to PostgreSQL
- A process group to manually export temporal data
- A process group to manually export the current state of entities

## Debugging and Troubleshooting

### Logging

The processor uses SLF4J for logging. Configure logging levels in your NiFi environment:

```xml
<logger name="egm.io.nifi.processors.ngsild" level="DEBUG"/>
```

## Resources

- Apache NiFi Documentation: https://nifi.apache.org/docs.html
- NGSI-LD Specification: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.08.01_60/gs_cim009v010801p.pdf
- PostgreSQL Documentation: https://www.postgresql.org/docs/

## Support

For questions or issues:

1. Check existing documentation
2. Review test cases for examples
3. Check the issue tracker for known problems
4. Submit new issues with detailed information
5. Contact project maintainers for complex questions
