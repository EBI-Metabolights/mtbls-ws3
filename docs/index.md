# Overview

## MetaboLights Ws3
<!-- <img src="https://www.ebi.ac.uk/metabolights/img/MetaboLightsLogo.png" width="50" height="50" alt="Metabolights">  -->
<a href="https:/www.ebi.ac.uk/metabolights" target="_blank">
    <img src="https://img.shields.io/badge/Homepage-MetaboLights-blue" alt="MetaboLights">
</a>
<a href="https://github.com/EBI-Metabolights/metabolights-utils" target="_blank">
    <img src="https://img.shields.io/badge/Github-MetaboLights-blue" alt="MetaboLights Github">
</a>
<a href="https://isa-specs.readthedocs.io/en/latest/isatab.html" target="_blank">
    <img src="https://img.shields.io/badge/ISA--Tab-v1.0-blue" alt="ISA-Tab version">
</a>
<a href="https://github.com/EBI-Metabolights/metabolights-utils/blob/master/LICENCE" target="_blank">
    <img src="https://img.shields.io/badge/Licence-Apache%20v2.0-blue" alt="License">
</a>

![Python](https://img.shields.io/badge/Python-3.8%7C3.9%7C3.10%7C3.11%7C3.12-dark_blue)
![Coverage](https://img.shields.io/badge/Coverage-85%25-dark_blue)
![Lint](https://img.shields.io/badge/Lint-Ruff-dark_blue)


---
The `mtbls-ws3` is Python application stack to manage MetaboLights open repository. It provides REST API endpoints for the metabolomics community and other MetaboLights applications.

---

## Features
* REST API endpoints with [FastAPI](https://github.com/fastapi/fastapi) and API documentation ([Redoc](https://github.com/Redocly/redoc)).
* Json serializable models with [pydantic](https://github.com/pydantic/pydantic) library.
* Serve the selected REST API endpoints (e.g., submission, curation, public, etc.).
* A new design with best practices, including dependency inversion, testability, modularity, maintainability, and extensibility.
* Jwt based extensible authentication, authorization, identity management
* Improved logging mechanism
