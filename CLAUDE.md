# Project Rules
- Always use Context7 MCP when I need library/API documentation, code generation, setup or configuration steps.
- For this project, always consult Context7 before using sqlglot APIs (AST node names, attributes, traversal, Spark dialect behavior).
- If Context7 is unavailable, proceed with conservative assumptions and leave TODO comments + add a failing unit test.
- Run python code using conda env ecse.