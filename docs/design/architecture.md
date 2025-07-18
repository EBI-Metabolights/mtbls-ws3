# Overview
MetaboLights WS3 is designed to address the latest goals and requirements of MetaboLights.

- `Separation of Concerns`: Different concerns (presentation , business logic and data access) should be isolated each other. Especially, business logic does not need to know how  data is stored or managed.
- `Maintainability`: Architecture should enable more maintainable code. To achieve this, Well defined layers should be maintained based on separation of concerns instead of external framework dependent spaghetti code. Layers and layer dependencies should be checked automatically.
- `Framework Independence`: Business logic should not depend on external frameworks to make it easy to upgrade them or use different frameworks without affecting remaining code. MetaboLights uses different frameworks and external services and over the years, any dependencies cause major issues for applications upgrades or infrastructure changes.
- `Testability`: Architecture should enable to change application configuration and run unit, integration, functional and non-functional tests.
- `Extensible`: Architecture should enable to add new components or new frameworks.
- `Flexible`: Architecture can enable to define and run different applications (multiple REST API servers, CLI applications) on same repository.
- `Scalable`: Each application should run as stateless  and can be deployed in a container.

/// tip | Inspiration

MetaboLights WS3 is inspired from the following software principals, architectures and approaches:

- SOLID (Single-responsibility, Open-closed Principle, Liskov substitution, Interface segregation, Dependency inversion) Object Oriented Design Principles
- Domain Driven Design (DDD)
- Command Query Responsibility Segregation (CQRS)
- Clean Architecture
- Onion Architecture
- Hexagonal / Port & Adaptor Architecture

///

## MetaboLights Ws3 Architecture


MetaboLights Ws3 architecture has  `domain`, `application`, `presentation` and `infrastructure` core layers, and an additional layer named `run` to customize endpoints and executables. You can find its layers and their dependency hierarchy below:

![MetaboLights Ws3 layers and dependency hierarchy](../assets/img/layers_dependencies.png){  style="display: block; margin: 0 auto" }


### General design principles

- Each layer can use the lower layer(s) but not higher layers. For example, `infrastructure` layer can import both `application` and `domain` layer modules but `application` layer can only import modules from `domain` layer.
- `infrastructure` and `presentation` layers are on the same level and they are isolated. They do not import any modules from each other.
- All external services (e.g., external web services) and python package dependencies (postgresql, redis, elasticsearch, etc.) are isolated from `application` layer modules. Application modules only use interfaces. Interfaces are implemented in `infrastructure` layer.
- `application` layer modules do not know how domain entities are stored and managed (on NFS, SQL or NoSQL DB, Object Storage, etc.). They use only repository interfaces to process them.
- Only `presentation` layer modules will check authentication or authorization (Role based authentication and authorization). There will be no logic to check authentication or authorization in `application` or `infrastructure` layer modules (They may access authentication and authorization information in read-only mode).
- All application logic should be implemented in `application` layer not in `presentation` or `infrastructure` layer.
- `presentation` layer modules should implement only RestAPI or CLI inputs and outputs (authentication and authorization). Any business logic should be moved to application and domain layer.
- Implementation layer modules should implement only application interfaces.
- Any external python package used in `presentation` or `infrastructure` layer is defined as an optional dependency (group).
- An `extra` dependency name is defined for each executable defined in `run` layer
```toml
[tool.poetry.extras]
ws3_worker = ["uvloop", "celery", "httpx", "sqlalchemy", "asyncpg", "metabolights-utils", "redis"]
```
- `dependency_injector` containers will be defined for each executable in `run` layer.
- Prefer to implement and use async coroutines for time consuming operations (external service calls, time consuming tasks etc.)

### Layers and design principles

#### Domain layer
Domain layer contains only MetaboLights specific domain classes, enumerations, decorators, exceptions and basic utility methods.

- `Domain` layer imports only `core Python packages` and the following libraries:
    * `pydantic`
    * `metabolights-utils`
    * Core utility libraries: `pyyaml`, `python-dateutil`, `pytz`

#### Application layer

- `Application` layer imports *only `domain` layer* packages. The only exception is async tasks in `application` layer. They use `dependency_injector` package for injection of services.

- All methods and classes in `application` layer use interfaces to access infrastructure components and services.

- `Application` layer modules do not import any external python libraries (e.g., celery, redis, etc.) in. Instead of importing any external library, a new service or component may be defined in `infrastructure` layer. To implement a new service or component:
    * Define interfaces in `application` layer.
    * Create any required domain objects in `domain` layer.
    * Implement interfaces in `infrastructure` layer.
    * Update `dependency_injector` container in `run` layer to use it.
- Initial interfaces in `application` layers are listed below:
    * `AsyncTaskService`
    * `AuthenticationService`, `AuthorizationService` and `IdentityService`
    * `CacheService`
    * `PolicyService`
    * `StudyMetadataService`
- Repositories are also defined as interfaces. e.g. `Study`, `User`, `ValidationReport`, `ValidationOverride`, `StudyObject`, etc. entity repositories.
- Application related logic is implemented in `use_cases` package. any module in `use_cases` package can import any other packages in `application` layer.
- All async tasks are implemented in `remote_tasks` package and they do not import `use_cases` modules.

You can find `application` layer packages and their dependency hierarchy below:

![`application` layer dependency hierarchy](../assets/img/application_layers.png){  style="display: block; margin: 0 auto" }


#### Presentation layer

- `Presentation` layer can imports `application` and `domain` layer packages. It may also import `dependency_injector` and presentation related libraries (e.g., FastAPI, click).
- `Presentation` layer do not import any `infrastructure` layer packages in `presentation` layer.
- Rest API, CLI or other executable can be defined as a presentation.
- Rest API endpoints are defined within API groups and each API group has managed versions. Initial Rest API groups are `submission`,`auth`, `curation`,  `submission`, and `public` .
    * `Submission Rest API group`: It provides endpoints to create and update MetaboLights submissions. Authentication is required to use endpoints.
    * `Curation Rest API group`: It provides endpoints to curators to run curation tasks on submitted studies and make them public. Authentication is required to use endpoints.
    * `Public Rest API group`: It provides endpoints to access MetaboLights studies.
    * `Auth Rest API group`: It provides to endpoints to create and revoke JWT tokens.
- Each user or service that wants to access any authorized endpoints should have a valid JWT token. Only JWT token is used to authorize requests.
- Rest API authentication will be managed within authorization middleware.


#### Infrastructure layer

- `Infrastructure` layer modules can import `dependency_injector` and any external libraries (e.g., redis, elasticsearch, celery, etc.) as well as `application` and `domain` layer.
- `Infrastructure` layer modules do not import any `presentation` layer modules.
- Multiple implementations can be defined for an interface defined in `application` layer. For example, there are proxy and standalone implementations for auth services. Each implementation is isolated from each others.
- Initial Service and repository implementations:
    * AsyncTaskService: `Celery` and `thread` (for development) implementations
    * AuthenticationService, AuthorizationService and IdentityService: `mtbls_ws2 proxy` and `standalone` implementations.
    * CacheService: `Redis`, `Redis sentinel` and `in-memory` (for development) implementations
    * PolicyService: Open policy agency (`OPA`) implementation
    * StudyMetadataService: `NFS` and `mongodb` (in progress) implementations
    * ValidationOverrideService, ValidationReportService: `NFS` and `mongodb` (in progress) implementations
    * Repositories:
        + `Study`, `Submitter`: `postgresql` and `sqlite` (for development) implementations
        + `ValidationReport`, `ValidationOverride`: `NFS` and `mongodb` (in progress)
        + `InvestigationFileObject`, `IsaTableObject` (`SampleFile`, `AssayFile` `AssignmentFile`): `NFS` and `mongodb` (in progress)
        + `FileObject`:  to store data folder content. `mongodb` (in progress)

#### Run layer

- `Run` layer defines `dependency_injector` container and runs executable in `presentation` layer (submission API, public API, worker, etc.).
- Initial executables are:
    * `Submission Rest API`: It uses celery async application, NFS and PostgreSQL based repositories, redis cache service and proxy authentication service.
    * `Submission Rest API Worker`: Celery worker to run submission remote tasks
    * `Submission Rest API Worker Monitor`: Flower executable to monitor celery tasks
- Log filters and application configuration files are customized depends on selected presentation layer application and service implementations.
- Unit tests overrides async task, auth, and cache services. Postgresql database is also overriden by Sqlite database to run unit tests.
