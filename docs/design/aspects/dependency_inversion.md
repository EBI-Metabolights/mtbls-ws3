# Dependency Inversion

Dependency injection is a common practice to decrease coupling and increase cohesion. It has three main advantages:

- `Flexibility`: It enables to develop loosely coupled components. An application can be extended or changed easily by using the components in a different way.
- `Testability`: Mocks can be injected easily instead of real services or resources (a service or database, etc.).
- `Maintainability`: Dependency injection helps to manage all dependencies. All components and dependencies defined explicitly in a container.


### General design and coding principles
- `dependency_injector` framework will be used for singleton, factory, configuration and resource  dependency injection capabilities.
- Dependency containers will be defined for each executable in `run` layer.
- Dependencies will be wired to only Rest API endpoints, async tasks and `run` layer modules. All others will not import dependency injection library need to define inputs to use interfaces managed in container.


### Dependency containers

Any dependency container will be created for

1. Selecting and defining service implementations and their dependencies (e.g. CacheService with RedisCacheService)
2. Selecting and defining repository implementations and their dependencies for domain entities (postgreSQL, NFS, Mongo)
3. Selecting and initiating application configuration (e.g. different config.yaml files for prod or development)
4. Initiating application level variables and triggering global functions (logging configuration, async task registry, request tracker, etc.)


/// example | Dependency container
You can find an example container definition below. It defines repositories and their implementations. It also assigns the implementations' initiation parameters as well.

///

```Python  hl_lines="11-12 18-19 25-26 32-33 43-44 49-50 55-56"
--8<-- "examples/api/dependency_injection/repository_container.py:44:130"
```



### Dependency injection to call Rest API endpoints

/// example
  The following example shows how a dependency is injected into a Rest API endpoint.
  Endpoint uses the injected service to call a business logic method in application layer

///

```Python  hl_lines="23-25 33-37"
--8<-- "examples/api/dependency_injection/override_endpoint.py:24:64"
```

/// example | Service usage in application layer

  Dependency wiring is not used in application layer (except async tasks). Any business method or class uses only interfaces.

///

```Python  hl_lines="4 6"
--8<-- "examples/api/dependency_injection/patch_validation_override.py:14:21"
```

### Dependency injection to call async tasks

/// example
  Async tasks are defined  in `application` layer and  can be called from `application` or `presentation` layer. They are entrypoints to run remote tasks on remote workers, so async tasks can define parameters to be injected by dependency injection mechanism.

  The following example shows an example usage of dependency injection in async tasks.
///

///

```Python  hl_lines="4 25 32-35"
--8<-- "examples/api/dependency_injection/run_validation_task.py:1:65"
```



### Unit tests and overrided dependency containers

/// example
  You can override containers any time but the most common case is unit tests.

  The following example shows an example how to override container to use:
  - local database
  - in-memory cache
  - standalone authentication service

  It also overrides configuration file to fetch configurations of local database, in-memory cache and standalone authentication service.

///


```Python  hl_lines="31 33 37 43"
--8<-- "examples/api/dependency_injection/override_container.py:1:65"
```
