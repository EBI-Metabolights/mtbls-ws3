# Authentication, Authorization and Identity Management

User authentication will be managed centrally for Rest API endpoints. Endpoints can check authentication information (authenticated or unauthenticated) to apply business rules (e.g. submitters can update study status if study is validated.)


### General design and coding principles
- Authentication can be completed with username/password or user API token. After authentication, a new JWT token will be created and shared with user.
- Users use the generated JWT tokens to access restricted endpoints.
- JWT tokens will be validated by authentication service. Neither other services nor endpoints will check JWT token.
- Authenticated or unauthenticated user information  will be stored in request context. Other services or endpoints can access it.
- If a request has a JWT token and it is not valid (e.g. invalid format, expired, etc.), authentication service will return error message.
- If the requested path contains resource id (MTBLS, REQ), authorization service checks permissions for the user. Other services or endpoints can access permission.



### Authentication
Authentication service methods are shown below. Implementations should support at least  JWT_TOKEN token and username/password authentications.
Default implementatations are listed below:

* `standalone`: Create and check JWT tokens. User information is fetched from database.
* `mtbls_ws2`: It is a proxy service and use mtbls_ws2 endpoints for authentications.


```Python  hl_lines="3 7 11 15"
--8<-- "examples/api/auth/authentication_service.py:5:63"
```


Authentication service is used by a middleware service, and UnauthenticatedUser / AuthenticatedUser object is injected to request.

```Python  hl_lines="12 16 19 39"
--8<-- "examples/api/auth/auth_backend.py:25:63"
```

Each endpoint can check UnauthenticatedUser / AuthenticatedUser object. if only authorized users can access, endpoint should raise `AuthenticationError` exception.


```Python  hl_lines="4 7 28"
--8<-- "examples/api/auth/check_permission.py:15:63"
```


### Authorization

The authorization service determines the user's permissions for the requested resource (MTBLSxxx or REQxxx) and can be used by any endpoint.

```Python  hl_lines="3-8 11-16"
--8<-- "examples/api/auth/authorization_service.py:8:63"
```
It returns `StudyPermissionContext` object that defines all possible user's permissions (read, write, delete, create).

```Python  hl_lines="2-5 10 12 16"
--8<-- "examples/api/auth/permission.py:13:63"
```

If there is a resource_id (or study_id) in the requested path (/../MTBLS1/..), `AuthorizationMiddleware` updates permission_context field of UnauthenticatedUser / AuthenticatedUser in request. Endpoints that include a resource_id in the request path can use it directly without requiring the authorization service. `AuthorizationMiddleware` also can check request paths and authorize request. If user has no permission to access the requested resource, `AuthorizationMiddleware` raises `AuthorizationError`. Path prefixes and allowed roles can be defined in configuration file.


/// example | Path Authorization Example


All endpoints starting with /submissions/ are restricted to users with the 'curator' or 'submitter' roles (some 'READ' exceptions if resource is public). Endpoints starting with /curation/ are accessible only to curators.

```
    authorized_endpoints:
    - prefix: "/submissions/"
      scopes:
      - curator
      - submitter
    - prefix: "/curation/"
      scopes:
      - curator
```

///


### Identity Management

User identities (username, password hash, role etc.) are stored on database.
