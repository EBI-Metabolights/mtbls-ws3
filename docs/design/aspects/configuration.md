# Configuration
Application configurations will be defined in yaml file(s) and managed by dependency injection mechanism.

### General design and coding principles
- The content and structure of a configuration file depend on the application's dependency container and selected service/repository implementations.
- Application dependency container loads configuration file and uses its content to initiate container elements (resource, service, repository, etc.).
- Any configuration related to business logic can be defined in config file. They can be injected by dependency injection mechanism.

/// example

The following example loads `config.yaml` file and sets it to core container. Core container uses `run.submission.logging` sub element.

On the other hand, Only `gateways` sub element of config file is set to gateways container and gateway container uses `gateways.database.postgresql.connection` sub element of config file.

///

Example Configuration File

```yaml

gateways:
  ...
  database:
    postgresql:
      connection:
        host: remote-host
        port: 32069
        user: test
        password: {{ postgresql.database }}
        database: sample
        url_scheme: postgresql+asyncpg
run:
  ...
  submission:
    logging:
      version: 1
      disable_existing_loggers: true
      formatters:
        json_formatter:
          format: '{ "level_name": "%(levelname)s", "time": "%(asctime)s",  "client": "%(client)s",  "path": "%(route_path)s", "resource_id": "%(resource_id)s", "user": %(user_id)d, "request_id": "%(request_id)s", "name": "%(name)s", "message": "%(message)s" }'
        text_formatter:
          format: '%(levelname)-8s %(asctime)s %(user_id)d %(client)s %(route_path)s %(resource_id)s %(request_id)s %(name)s "%(message)s"'
      handlers:
        console:
          class: "logging.StreamHandler"
          level: DEBUG
          formatter: "json_formatter"
          stream: "ext://sys.stdout"
          filters: [ default_filter, correlation_id ]
      root:
        level: DEBUG
        handlers: [ "console" ]
        propogate: true
      loggers:
        mtbls:
          level: DEBUG
          propogate: yes
      ...
```

Example Container Definition

```Python hl_lines="6 14 19 23 28"

class Ws3CoreContainer(containers.DeclarativeContainer):
    config = providers.Configuration()

    logging_config = providers.Resource(
        logging_config.dictConfig,
        config=config.run.submission.logging,
    )
    async_task_registry = providers.Resource(get_async_task_registry)

class GatewaysContainer(containers.DeclarativeContainer):
    config = providers.Configuration()
    database_client: DatabaseClient = providers.Singleton(
        DatabaseClientImpl,
        db_connection=config.database.postgresql.connection,
        db_pool_size=runtime_config.db_pool_size,
    )

class Ws3ApplicationContainer(containers.DeclarativeContainer):
    config = providers.Configuration(yaml_files=["config.yaml"])

    core = providers.Container(
        Ws3CoreContainer,
        config=config,
    )

    gateways = providers.Container(
        GatewaysContainer,
        config=config.gateways,
    )
```

### Secrets
Secrets are stored in a different yaml file named `config-secrets.yaml`. They are referenced in config file as a template (e.g.`{{ postgresql.password }}` ) and rendered with `Jinja2` template framework.

Each application should render config file after creating dependency container.

```Python  hl_lines="2-3 13"
class Ws3ApplicationContainer(containers.DeclarativeContainer):
    config = providers.Configuration(yaml_files=["config.yaml"])
    secrets = providers.Configuration(yaml_files=["config-secrets.yaml"])
    core = providers.Container(
        Ws3CoreContainer,
        config=config,
    )

...
# initiate container and render secrets
container = Ws3ApplicationContainer()
container.init_resources()
render_config_secrets(container.config(), container.secrets())
...
```

/// example | Example config.yaml

```yaml  hl_lines="9"

gateways:
  ...
  database:
    postgresql:
      connection:
        host: remote-host
        port: 32069
        user: xyz
        password: {{ postgresql.database }}
        database: testdb
        url_scheme: postgresql+asyncpg
```

///

/// example | Example config-secrets.yaml

```yaml hl_lines="4"
redis:
  password: redis_ws1
postgresql:
  password: test.123
standalone_authentication:
  application_secret_key: application-test.123
```

///
