## Code Style Guidelines

### General Principles

- Write clean, readable, idiomatic Go code
- Keep functions short and focused (single responsibility)
- Use meaningful variable and function names
- Follow the [Go Code Review Comments](https://github.com/golang/go/wiki/CodeReviewComments) style guide


### Imports

- Use the standard Go import organization:
  1. Standard library packages
  2. Third-party packages
  3. Internal packages

- Use blank imports (`_`) only when necessary for side effects
- Use dot imports (`.`) only in test files (`_test.go`)

### Error Handling

- Always handle errors explicitly; never ignore them with `_`
- Return meaningful errors with context using `fmt.Errorf` or `errors.Wrap`
- Use sentinel errors for expected error conditions
- Check errors early and return early (fail fast)
- Avoid generic error messages; be specific

### Logging

- Use structured logging with appropriate levels (debug, info, warn, error)
- Include relevant context in log messages
- Use `log/slog` from standard library
- Avoid logging sensitive information (passwords, tokens, keys)
- Do not use custom loggers, just call directly e.g. slog.Info(..)



# Preferred dependencies (Go)

- `github.com/alecthomas/kong` - CLI argument parsing
- `golangci/golangci-lint` - Linting
- `gotest.tools/v3` - Testing infrastructure
