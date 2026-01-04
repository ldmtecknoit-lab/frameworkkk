# SottoMonte Framework: LLM Context Reference

This document provides essential context for LLMs to understand, maintain, and extend the SottoMonte framework.

## 🏛️ Architecture Overview

The framework follows a **Ports and Adapters (Hexagonal Architecture)** pattern combined with a **Contract-Driven Dependency Filter (CDDF)**.

- **Ports**: Definitions of interfaces (found in `src/framework/port/`).
- **Adapters**: Concrete implementations (found in `src/infrastructure/`).
- **Managers**: Orchestrators that tie ports together (found in `src/framework/manager/`).
- **Services**: Core logic for loading, flow control, and DSL parsing (found in `src/framework/service/`).
- **Pipelines**: The framework uses `flow.pipe` for functional, declarative execution of tasks.

## 🛡️ CDDF (Contract-Driven Dependency Filter)

This is the core security and integrity mechanism of the framework.

1.  **Contracts**: Each module `.py` has a corresponding `.test.py` and a generated `.contract.json`.
2.  **Integrity**: At runtime, `load.py` calculates the BLAKE2b hash of the functions being loaded and compares them against the `production` hash in `.contract.json`.
3.  **Filtering**: Only methods that are explicitly tested in `.test.py` and have matching hashes in `.contract.json` are exposed to the rest of the application.
4.  **Exports**: The `.test.py` file must define an `exports` dictionary to specify which members are public.

## 📜 DSL & Bootstrapping

The framework is initialized via `src/framework/service/bootstrap.dsl`.

- **DSL Syntax**: A custom language parsed by `language.py` using Lark. It supports piping (`|`), dictionaries, tuples, and function calls.
- **Resource Loading**: `load.py` handles the loading of both Python modules and DSL files.
- **DI Registration**: The `register` function in `load.py` populates a global `dependency-injector` container.

## 🛠️ Maintenance & Development

### Regenerating Contracts
If you modify code in `src/`, you **MUST** update the corresponding test in `.test.py` and then regenerate the contracts:
```bash
python3 generate_contracts.py
```
Failure to do this will result in "Hash mismatch" errors at runtime, as the loader will refuse to load modified code that doesn't match its contract.

### Running the Application
The main entry point is `public/main.py`:
```bash
python3 public/main.py
```

### Key Files
- [load.py](file:///home/asd/framework/src/framework/service/load.py): The heart of the CDDF and resource loading.
- [language.py](file:///home/asd/framework/src/framework/service/language.py): DSL grammar and execution visitor.
- [flow.py](file:///home/asd/framework/src/framework/service/flow.py): Functional pipeline implementation.
- [bootstrap.dsl](file:///home/asd/framework/src/framework/service/bootstrap.dsl): Framework startup configuration.

## 📝 Coding Standards
- **Expose via Exports**: Always update `exports` in `.test.py` when adding new public functions/classes.
- **Test-Driven**: A module without a `.test.py` cannot have its members exported by the CDDF (unless Auto-Trust is explicitly triggered, which is discouraged).
- **Use Pipelines**: Prefer `flow.pipe` and `flow.step` for complex asynchronous logic.
