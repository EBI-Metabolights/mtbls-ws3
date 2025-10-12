# Overview

MetaboLights WS3 is designed to address the latest goals and requirements of MetaboLights.

- `Separation of Concerns`: Each concern (presentation, business logic, and data access) should be clearly separated. Specifically, the business logic should remain independent of the details of data storage and management.
- `Maintainability`: The architecture must support long-term maintainability. This requires clearly defined layers that adhere to the principle of separation of concerns, avoiding framework-dependent or tightly coupled (“spaghetti”) code. Layer structures and their dependencies should be automatically validated to ensure architectural integrity.
- `Framework Independence`: The business logic layer must remain independent of external frameworks. This promotes flexibility, allowing frameworks to be upgraded or substituted with minimal impact on the rest of the system. In MetaboLights, reliance on specific frameworks and external services has historically led to major issues during application and infrastructure upgrades.
- `Testability`: The architecture should allow application configurations to be easily modified and support the execution of unit, integration, functional, and non-functional tests.
- `Extensible`: The architecture should be extensible, allowing new components or frameworks to be added with minimal effort.
- `Flexible`: The architecture should support the definition and execution of multiple application types—such as REST API servers and command-line interfaces—within a single codebase or repository.
- `Scalable`: Each application should operate as a stateless service that can be easily deployed within a containerized infrastructure.

/// tip | Inspiration

MetaboLights WS3 is inspired from the following software principals, architectures and approaches:

- [SOLID](https://en.wikipedia.org/wiki/SOLID){:target="\_blank"} (Single-responsibility, Open-closed Principle, Liskov substitution, Interface segregation, Dependency inversion) Object Oriented Design Principles.
- [Domain Driven Design (DDD)](https://en.wikipedia.org/wiki/Domain-driven_design){:target="\_blank"}
- [Command Query Responsibility Segregation (CQRS)](https://martinfowler.com/bliki/CQRS.html){:target="\_blank"}
- [Clean Architecture](https://blog.cleancoder.com/uncle-bob/2012/08/13/the-clean-architecture.html){:target="\_blank"}
- Onion Architecture
- [Hexagonal / Port & Adaptor Architecture](<https://en.wikipedia.org/wiki/Hexagonal_architecture_(software)>){:target="\_blank"}

///

## MetaboLights WS3 Architecture

The MetaboLights WS3 architecture consists of four core layers — `domain`, `application`, `presentation`, and `infrastructure` — along with an additional `run` layer used to customize endpoints and executables. The layers and their dependency hierarchy are illustrated below.

![MetaboLights WS3 layers and dependency hierarchy](../assets/img/layers_dependencies.png){ style="display: block; margin: 0 auto" }

### General design principles

- Each layer can use the lower layer(s) but not higher layers. For example, `infrastructure` layer can import both `application` and `domain` layer modules, however `application` layer can only import modules from `domain` layer.
- `infrastructure` and `presentation` layers are on the same level and they are isolated. They do not import any modules from each other.
- All external services (e.g., external web services) and python package dependencies (postgresql, redis, elasticsearch, etc.) are isolated from `application` layer modules. Application modules only use interfaces. Interfaces are implemented in `infrastructure` layer. For example, CacheService interface in `application` layer can be implemented with different frameworks (RedisCache, InMemoryCache, RedisSentinelCache, etc.) in `infrastructure`.
- `application` layer modules do not know how domain entities are stored and managed (on NFS, SQL or NoSQL DB, Object Storage, etc.). They use only repository interfaces to process them.
- Only `presentation` layer modules will check authentication or authorization (Role based authentication and authorization). There will be no logic to check authentication or authorization in `application` or `infrastructure` layer modules (They may access authentication and authorization information in read-only mode).
- All application logic should be implemented in `application` layer (not in `presentation` or `infrastructure` layer).
- `presentation` layer modules should implement only RestAPI or CLI inputs and outputs (authentication and authorization). Any business logic should be moved to application and domain layer.
- `Infrastructure` layer modules should implement only application interfaces. For example, HttpClient interface has `send_request` method and HttpxClient in `Infrastructure` layer implements send_request method using httpx module.
- Do not use any external python package dependencies in `domain` or `application` layer (Exceptions: pydantic).
- `dependency_injector` containers will be defined for each executable (ws, ws_worker, cli, etc) in `run` layer.
- Prefer async coroutines for time consuming operations (external service calls, time consuming tasks etc.)

### Layers and design principles

#### Domain layer

Domain layer contains only MetaboLights specific domain classes, enumerations, decorators, exceptions and basic utility methods.

- `Domain` layer imports only `core Python packages` and the following libraries:
  - `pydantic`
  - `metabolights-utils`
  - Basic utility libraries: `pyyaml`, `python-dateutil`, `pytz`

#### Application layer

- `Application` layer imports *only `domain` layer* packages. The only exception is `dependency_injector` to define async tasks.

- All methods and classes in `application` layer use interfaces to access infrastructure components and services.

- `Application` layer modules do not import any external python libraries (e.g., celery, redis, sqlachemy, etc.). Instead of importing any external library, a new service or component can be implemented in `infrastructure` layer. To implement a new service or component:

  - Define interfaces in `application` layer.
  - Create any required domain objects in `domain` layer.
  - Implement interfaces in `infrastructure` layer.
  - Update `dependency_injector` container in `run` layer to use it.

- Initial interfaces in `application` layers are listed below:

  - `HttpClient`
  - `AsyncTaskService`
  - `AuthenticationService`, `AuthorizationService` and `IdentityService`
  - `CacheService`
  - `PolicyService`
  - `StudyMetadataService`

- Repositories are also defined as interfaces. e.g. `Study`, `User`, `ValidationReport`, `ValidationOverride`, `StudyFile`, etc. entity repositories.

- Application related logic is implemented in `use_cases` package. any module in `use_cases` package can import any other packages in `application` layer.

- All async tasks are implemented in `remote_tasks` package and they do not import `use_cases` modules.

You can find `application` layer packages and their dependency hierarchy below:

![ layer dependency hierarchy](../assets/img/application_layers.png){ style="display: block; margin: 0 auto" }

#### Presentation layer

- `Presentation` layer can imports `application` and `domain` layer packages. It may also import `dependency_injector` and presentation related libraries (e.g., FastAPI, click).
- `Presentation` layer do not import any `infrastructure` layer packages packages and modules.
- Rest API, CLI or other executable can be defined as a presentation.
- Rest API endpoints are defined within API groups and each API group will be managed with versions. Initial Rest API groups are `submission`,`auth`, `curation`, `system`, and `public` .
  - `Submission Rest API group`: It provides endpoints to create and update MetaboLights submissions. Authentication is required to use endpoints.
  - `Curation Rest API group`: It provides endpoints to run curation tasks on submitted studies and make them public. Authentication is required to use endpoints.
  - `Public Rest API group`: It provides endpoints to access MetaboLights public studies and statistics.
  - `Auth Rest API group`: It provides to endpoints to create and revoke API tokens (JWT tokens).
  - `System Rest API group`: It provides to endpoints to MetaboLights sytem related endpoints (Private FTP accessibility, etc.).
- Only JWT token is used to authorize requests. Each user or service that wants to access any authorized endpoints should have a valid JWT token.
- Rest API authentication will be managed within authorization middleware.

#### Infrastructure layer

- `Infrastructure` layer modules can import `application` and `domain` layer modules, `dependency_injector` module and any external library (e.g., redis, slqalchemy, elasticsearch, celery, etc.).
- `Infrastructure` layer modules do not import any `presentation` layer modules.
- Multiple implementations can be defined for an interface defined in `application` layer. Each implementation is isolated from each others.
- Initial Service and repository implementations:
  - AsyncTaskService: `Celery` and `thread` (for development) implementations
  - AuthenticationService, AuthorizationService and IdentityService: `mtbls_ws2 proxy` and `standalone` implementations.
  - CacheService: `Redis`, `Redis sentinel` and `in-memory` (for development) implementations
  - PolicyService: Open policy agency (`OPA`) implementation
  - StudyMetadataService: `NFS` and `mongodb` (in progress) implementations
  - ValidationOverrideService, ValidationReportService: `NFS` and `mongodb` (in progress) implementations
  - Repositories:
    - `Study` & `User`: `postgresql` and `sqlite` (for development) implementations
    - `ValidationReport` & `ValidationOverride`: `NFS` and `mongodb` (in progress)
    - `InvestigationFileObject`, `IsaTableObject` (`SampleFile`, `AssayFile` `AssignmentFile`): `NFS` and `mongodb` (in progress)
    - `FileObject`: to store data folder content. `NFS` (in progress) and `mongodb` (in progress)

#### Run layer

- `Run` layer defines `dependency_injector` container and runs an executable in `presentation` layer (submission API, submission API worker, CLI, etc.).
- Initial executables are:
  - `Submission Rest API`: It uses celery async application, NFS and PostgreSQL based repositories, redis cache service and proxy authentication service.
  - `Submission Rest API Worker`: Celery worker to run submission remote tasks
  - `Submission Rest API Worker Monitor`: Flower executable to monitor celery tasks
  - `CLI`: Initial commandline tool named `mtbls-tools`
- Log filters and application configuration files are customized depends on selected presentation layer application and service implementations.
- Unit tests overrides async task, auth, and cache services. Postgresql database is also overriden by Sqlite database to run unit tests.
