# Project Tools and Commands

## Task Runner

- `task -l` - List available tasks
- `task run-simple & sleep 5 && kill -9 $(lsof -t -i :6033)` - Run application
  for 5 seconds then terminate properly
- `task test` - Run all tests
- `task docker` - Run the application using Docker
- `task build` - Build the project binary
- `task swagger` - Generate swagger spec

## Git Rules

- Do not add any new files to git or create commits without previous authorization
- Always use the Conventional Commits format described below when writing
  commit messages

## On Code Generation

- Generate readable code
- Reduce code duplication to the minimum
- Compose functions rather than have full implementation on a single function

### Conventional Commits Summary

Commit Message Format:

```
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```

Common Commit Types:

- `feat:` - Introduces a new feature (correlates with MINOR version)
- `fix:` - Patches a bug (correlates with PATCH version)
- `BREAKING CHANGE:` - Introduces a breaking API change (correlates with MAJOR version)

Additional Optional Types:

- `build:`, `chore:`, `ci:`, `docs:`, `style:`, `refactor:`, `perf:`, `test:`

Example Commits:

1. Simple feature:

```
feat: add user authentication
```

2. Breaking change:

```
feat(api)!: send email on product shipment

BREAKING CHANGE: Requires new email configuration
```
