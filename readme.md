## How to run our program:

Requires Go (see the version in `go.mod`). Run everything from the project root (log files are written to the current
directory). Use one terminal per process.

**1. Start the server** (listens on `localhost:8080`)

```
go run ./server
```

**2. Start at least three clients**, each in its own terminal, each with a
unique name:

```
go run ./client -name Daniel
go run ./client -name Filip
go run ./client -name Simon
```

Each client logs to `client-<name>.log`.

**3. Chat**

- Type a message and press Enter to publish it (max 128 UTF-8 characters).
- Type `/leave` or press Ctrl+C to leave.
- Stop the server with Ctrl+C in the server terminal.

Every received message is shown as `[<lamport time>] <sender>: <content>`.