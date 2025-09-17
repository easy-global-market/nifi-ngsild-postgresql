# NiFi NGSI-LD PostgreSQL Processor

## Presentation

The NiFi NGSI-LD PostgreSQL Processor is a specialized Apache NiFi extension that provides seamless integration between
FIWARE Context Brokers and PostgreSQL databases. This Generic Enabler automatically transforms NGSI-LD data into
structured relational database tables, enabling real-time data warehousing and analytics capabilities for FIWARE ecosystems.

Key features include:
- **Automatic Database Schema Generation**: Dynamically creates PostgreSQL tables based on NGSI-LD entity structures, eliminating manual database design
- **NGSI-LD Standard Compliance**: Full support for NGSI-LD temporal data, multi-attributes, relationships, and system attributes (createdAt, modifiedAt, observedAt)
- **Flexible Data Transformation**: Configurable flattening of observations, attribute filtering, and customizable table naming conventions
- **Enterprise-Grade Processing**: Batch processing support, connection pooling, error handling, and retry mechanisms for production environments
- **Temporal Data Support**: Native handling of time-series data with proper temporal attribute management
- **First level relationships support**: Leverage Linked Entity Retrieval to export data for first-level relationships into the same table as the main entity

The processor integrates seamlessly with existing NiFi data flows, allowing organizations to build comprehensive data
pipelines that connect FIWARE-enabled IoT devices and applications directly to their analytical infrastructure.

## Why using it?

Modern smart city and IoT deployments generate massive volumes of context data that need to be stored, analyzed, and
visualized for actionable insights. Traditional approaches require complex custom integrations between FIWARE components
and database systems, leading to development overhead and maintenance challenges.

This Generic Enabler eliminates the integration complexity by providing a production-ready, standards-compliant solution
that bridges the gap between FIWARE's context management capabilities and enterprise data analytics platforms.
Organizations can immediately start collecting and analyzing their NGSI-LD data without investing in custom development
or dealing with format conversion complexities.

## Developer guidelines

### Overview

The NiFi NGSI-LD PostgreSQL Processor is a specialized Apache NiFi extension that transforms NGSI-LD data streams into structured PostgreSQL database tables. This guide provides developers with the information needed to understand, build, modify, and contribute to the project.

### Prerequisites

- **Java Development Kit (JDK) 21 or higher**
- **Apache Maven 3.6.0 or higher**
- **Docker** (for running integration tests with TestContainers)
- **PostgreSQL 16 or higher, with PostGIS extension** (for local development and testing)
- **Git** for version control
- **IDE** with Maven support (IntelliJ IDEA, Eclipse, VS Code)

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

1. **Clone the repository:**
   ```bash
   git clone https://github.com/easy-global-market/nifi-ngsild-postgresql
   cd nifi-ngsild-postgresql
   ```

2. **Build the entire project:**
   ```bash
   mvn clean compile
   ```

3. **Run tests:**
   ```bash
   mvn test
   ```
   Note: Integration tests require Docker to be running as they use TestContainers for PostgreSQL.

4. **Package the NAR file:**
   ```bash
   mvn clean package
   ```
   This creates the deployable NAR file in `nifi-ngsild-postgresql-nar/target/`.

### Running Tests

The project includes comprehensive unit and integration tests:

- **Unit Tests**: Test individual components and logic
- **Integration Tests**: Use TestContainers to spin up PostgreSQL instances for realistic testing

**Test execution:**
```bash
# Run all tests
mvn test

# Run specific test class
mvn test -Dtest=TestNgsiLdToPostgreSQL

# Skip integration tests (if Docker is not available)
mvn test -DskipITs=true
```

**Test Configuration:**
- Tests use `postgis/postgis:16-3.5-alpine` Docker image
- Automatic database cleanup after each test
- Mock NiFi environment for processor testing

## Installation Guidelines

### System Requirements

- **Operating System**: Linux, macOS, or Windows
- **Java Runtime Environment (JRE)**: Java 21 or higher
- **Apache NiFi**: Version 2.4.0 or higher
- **PostgreSQL Database**: Version 16 or higher, with PostGIS extension
- **Memory**: Minimum 4GB RAM recommended for NiFi with this processor
- **Storage**: Sufficient disk space for NiFi installation and database storage

### Software Dependencies

1. **Apache NiFi 2.4.0+**
    - Download from: https://nifi.apache.org/download/
    - Follow NiFi installation guide for your platform

2. **PostgreSQL Database**
    - Can be local or remote installation
    - Ensure network connectivity from NiFi instance
    - Required permissions: CREATE SCHEMA, CREATE TABLE, INSERT, SELECT

3. **PostgreSQL JDBC Driver**
    - Version 42.7.7 or compatible recommended

### Installation Methods

#### Method 1: Pre-built NAR Installation

1. **Download the NAR file:**
   ```bash
   wget https://github.com/easy-global-market/nifi-ngsild-postgresql/releases/download/2.4.0/nifi-ngsild-postgresql-nar-2.4.0.nar
   ```

2. **Copy NAR to NiFi:**
   ```bash
   cp nifi-ngsild-postgresql-nar-2.4.0.nar $NIFI_HOME/lib/
   ```

3. **Restart NiFi:**
   ```bash
   $NIFI_HOME/bin/nifi.sh restart
   ```

#### Method 2: Build from Source

1. **Prerequisites for building:**
    - Java Development Kit (JDK) 21
    - Apache Maven 3.6.0+
    - Git

2. **Clone and build:**
   ```bash
   git clone https://github.com/easy-global-market/nifi-ngsild-postgresql
   cd nifi-ngsild-postgresql
   mvn clean package
   ```

3. **Install the built NAR:**
   ```bash
   cp nifi-ngsild-postgresql-nar/target/nifi-ngsild-postgresql-nar-2.4.0.nar $NIFI_HOME/lib/
   ```

4. **Restart NiFi:**
   ```bash
   $NIFI_HOME/bin/nifi.sh restart
   ```

### Method 3: Docker Environment

1. **Create Dockerfile extending NiFi:**
   ```dockerfile
   FROM apache/nifi:2.4.0
   COPY nifi-ngsild-postgresql-nar-2.4.0.nar /opt/nifi/nifi-current/lib/
   ```

2. **Build and run:**
   ```bash
   docker build -t nifi-ngsild-postgresql .
   docker run -d -p 8443:8443 nifi-ngsild-postgresql
   ```

## NiFi Configuration

### Database Connection Pool Setup

1. **Access NiFi UI:**
    - Navigate to https://localhost:8443/nifi
    - Login with NiFi credentials

2. **Create DBCPConnectionPool Service:**
    - Go to Controller Services
    - Add Controller Service → DBCPConnectionPool
    - Configure properties:

   | Property | Value | Example |
   |----------|--------|---------|
   | Database Connection URL | jdbc:postgresql://host:port/database | jdbc:postgresql://localhost:5432/nifi_data |
   | Database Driver Class Name | org.postgresql.Driver | org.postgresql.Driver |
   | Database User | nifi_user | nifi_user |
   | Password | secure_password | *********** |
   | Max Wait Time | 500 millis | 500 millis |
   | Max Total Connections | 8 | 8 |

3. **Enable the service:**
    - Click enable
    - Verify service shows "Enabled" status

### Processor Configuration

1. **Configure Required Properties:**

   | Property | Description | Default | Example |
   |----------|-------------|---------|---------|
   | JDBC Connection Pool | Reference to DBCPConnectionPool | (required) | DBCPConnectionPool |
   | Batch Size | FlowFiles per transaction | 10 | 100 |

2. **Configure Optional Properties:**

   | Property | Description | Default | Recommended |
   |----------|-------------|---------|-------------|
   | DB Schema | Target database schema | stellio | stellio |
   | Table Name Suffix | Suffix for table names | (none) | _data |
   | Flatten Observations | Generic observation columns | false | false |
   | Ignore Empty Observed At | Skip records without timestamps | true | true |
   | Export System Attributes | Include createdAt, modifiedAt | false | true |
   | Ignored Attributes | Comma-separated attribute list | (none) | id,type |

## Debugging and Troubleshooting

### Logging

The processor uses SLF4J for logging. Configure logging levels in your NiFi environment:

```xml
<!-- For debugging -->
<logger name="egm.io.nifi.processors.ngsild" level="DEBUG"/>
```

## Resources

- **Apache NiFi Documentation**: https://nifi.apache.org/docs.html
- **NGSI-LD Specification**: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.08.01_60/gs_cim009v010801p.pdf
- **PostgreSQL Documentation**: https://www.postgresql.org/docs/

## Support

For questions or issues:

1. Check existing documentation
2. Review test cases for examples
3. Check the issue tracker for known problems
4. Submit new issues with detailed information
5. Contact project maintainers for complex questions
