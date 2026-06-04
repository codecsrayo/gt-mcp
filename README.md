# gt-mcp (`gtmcp`)

Minimal Rust CLI to invoke **gt-core MCP** tools over Streamable HTTP from the shell.

## The problem it solves

The gt-core MCP server (57 tools + 2 resources) speaks **Streamable HTTP**: a request
is only accepted after a stateful handshake — `initialize` → read the `Mcp-Session-Id`
response header → `notifications/initialized` → then the actual call, with responses
streamed back as **SSE** (`text/event-stream`), not plain JSON.

Claude Code's deferred-tool loader does not complete that handshake, so the tools never
load in the VSCode client. The documented workaround was a hand-rolled Python `requests`
snippet pasted per call — easy to get wrong, no session reuse, manual SSE parsing.

`gtmcp` collapses all of that into one binary:

- Runs the full `initialize` → `notifications/initialized` handshake and captures
  `Mcp-Session-Id` automatically.
- Parses the SSE response stream and prints the JSON-RPC `result`.
- One **generic** command — `call <tool> <json>` — reaches all 57 tools with no
  per-tool boilerplate, so new server tools work the instant they ship.
- Easiest surface for an LLM agent: one command pattern, JSON args passed through
  verbatim (no flag mapping for nested `domain[]` / `depends_on[]`).

Each invocation is one stateless session.

## Build

```sh
cargo build --release
# binary: target/release/gtmcp
```

## Config (env or flags)

| Flag | Env | Default |
|------|-----|---------|
| `--url` | `GT_MCP_URL` | `http://127.0.0.1:8765/mcp` |
| `--actor` | `GT_MCP_ACTOR` | `mcp-local` |
| `--workspace` | `GT_MCP_WORKSPACE` | `acme` |
| `--compact` | — | pretty JSON off |

## Usage

```sh
# list tools (name + description)
gtmcp list
gtmcp list --full              # full input schemas

# list / read resources
gtmcp resources
gtmcp resource 'gt://issues?status=open&limit=10'
gtmcp resource 'gt://issue/hq-123'

# call a tool — args as positional JSON, --file, or stdin ('-')
gtmcp call meta_help_execute
gtmcp call issues_create_validate '{"id":"hq-x","title":"t","issue_type":"task","created_by":"me"}'
gtmcp call issues_update_execute --file patch.json
echo '{"session":"s1","rig":"r1"}' | gtmcp call agent_spawn -

# raw JSON-RPC passthrough
gtmcp raw tools/list '{}'
```

Prints the JSON-RPC `result`. Non-zero exit + stderr on HTTP / JSON-RPC error.
