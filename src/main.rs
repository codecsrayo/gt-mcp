//! gtmcp — minimal CLI for the gt-core MCP server (Streamable HTTP / SSE).
//!
//! Each invocation is one stateless session: it performs the MCP `initialize`
//! handshake (capturing `Mcp-Session-Id`), sends `notifications/initialized`,
//! then issues the requested call and prints the JSON-RPC `result`.

use anyhow::{anyhow, bail, Context, Result};
use clap::{Parser, Subcommand};
use serde_json::{json, Value};
use std::io::Read;

const PROTOCOL_VERSION: &str = "2024-11-05";

#[derive(Parser)]
#[command(
    name = "gtmcp",
    about = "Invoke gt-core MCP tools over Streamable HTTP",
    version
)]
struct Cli {
    /// MCP endpoint URL
    #[arg(long, env = "GT_MCP_URL", default_value = "https://gt.codecsrayo.com/mcp")]
    url: String,

    /// X-Actor header
    #[arg(long, env = "GT_MCP_ACTOR", default_value = "mcp-local")]
    actor: String,

    /// X-Workspace header
    #[arg(long, env = "GT_MCP_WORKSPACE", default_value = "acme")]
    workspace: String,

    /// Print compact (single-line) JSON instead of pretty
    #[arg(long, global = true)]
    compact: bool,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Call a tool: gtmcp call <tool> '<json-args>'  (args from positional, --file, or stdin via '-')
    Call {
        /// Tool name, e.g. issues_create_execute
        tool: String,
        /// JSON arguments object. Defaults to {}. Use '-' to read from stdin.
        #[arg(default_value = "{}")]
        args: String,
        /// Read JSON arguments from a file instead of the positional arg
        #[arg(long)]
        file: Option<String>,
    },
    /// List available tools (name + description)
    List {
        /// Print full tools/list JSON (with input schemas)
        #[arg(long)]
        full: bool,
    },
    /// List available resources
    Resources,
    /// Read a resource by URI, e.g. gtmcp resource 'gt://issues?limit=10'
    Resource { uri: String },
    /// Raw JSON-RPC passthrough: gtmcp raw <method> '<json-params>'
    Raw {
        method: String,
        #[arg(default_value = "{}")]
        params: String,
    },
}

struct Mcp {
    http: reqwest::blocking::Client,
    url: String,
    actor: String,
    workspace: String,
    session: Option<String>,
    next_id: i64,
}

impl Mcp {
    fn new(cli: &Cli) -> Result<Self> {
        Ok(Self {
            http: reqwest::blocking::Client::builder()
                .build()
                .context("build http client")?,
            url: cli.url.clone(),
            actor: cli.actor.clone(),
            workspace: cli.workspace.clone(),
            session: None,
            next_id: 0,
        })
    }

    fn id(&mut self) -> i64 {
        self.next_id += 1;
        self.next_id
    }

    /// POST a JSON-RPC request, parse JSON or SSE body, return the message.
    fn post(&mut self, body: &Value, expect_response: bool) -> Result<Value> {
        let mut req = self
            .http
            .post(&self.url)
            .header("Content-Type", "application/json")
            .header("Accept", "application/json, text/event-stream")
            .header("X-Actor", &self.actor)
            .header("X-Workspace", &self.workspace);
        if let Some(s) = &self.session {
            req = req.header("Mcp-Session-Id", s);
        }

        let resp = req.json(body).send().context("send request")?;
        let status = resp.status();

        // Capture session id on first (initialize) response.
        if self.session.is_none() {
            if let Some(v) = resp.headers().get("Mcp-Session-Id") {
                if let Ok(s) = v.to_str() {
                    self.session = Some(s.to_string());
                }
            }
        }

        let ctype = resp
            .headers()
            .get("Content-Type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        let text = resp.text().context("read body")?;

        if !status.is_success() {
            bail!("HTTP {status}: {text}");
        }
        if !expect_response {
            return Ok(Value::Null);
        }

        let msg = if ctype.contains("text/event-stream") {
            parse_sse(&text)?
        } else if text.trim().is_empty() {
            bail!("empty response body");
        } else {
            serde_json::from_str(&text).with_context(|| format!("parse JSON body: {text}"))?
        };

        if let Some(err) = msg.get("error") {
            bail!("JSON-RPC error: {err}");
        }
        Ok(msg.get("result").cloned().unwrap_or(msg))
    }

    fn initialize(&mut self) -> Result<()> {
        let id = self.id();
        self.post(
            &json!({
                "jsonrpc": "2.0", "id": id, "method": "initialize",
                "params": {
                    "protocolVersion": PROTOCOL_VERSION,
                    "capabilities": {},
                    "clientInfo": {"name": "gtmcp", "version": env!("CARGO_PKG_VERSION")}
                }
            }),
            true,
        )?;
        // Notify server the handshake is complete (notification: no response).
        self.post(
            &json!({"jsonrpc": "2.0", "method": "notifications/initialized"}),
            false,
        )?;
        Ok(())
    }

    fn rpc(&mut self, method: &str, params: Value) -> Result<Value> {
        let id = self.id();
        self.post(
            &json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}),
            true,
        )
    }
}

/// Extract the JSON-RPC message from an SSE stream (concatenated `data:` fields,
/// last parseable event wins).
fn parse_sse(text: &str) -> Result<Value> {
    let mut last: Option<Value> = None;
    let mut data = String::new();
    fn flush(data: &mut String, last: &mut Option<Value>) {
        if !data.is_empty() {
            if let Ok(v) = serde_json::from_str::<Value>(data.trim()) {
                *last = Some(v);
            }
            data.clear();
        }
    }
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("data:") {
            if !data.is_empty() {
                data.push('\n');
            }
            data.push_str(rest.trim_start());
        } else if line.trim().is_empty() {
            flush(&mut data, &mut last);
        }
    }
    flush(&mut data, &mut last);
    last.ok_or_else(|| anyhow!("no JSON message found in SSE stream:\n{text}"))
}

fn read_args(args: &str, file: &Option<String>) -> Result<Value> {
    let raw = if let Some(path) = file {
        std::fs::read_to_string(path).with_context(|| format!("read {path}"))?
    } else if args == "-" {
        let mut buf = String::new();
        std::io::stdin().read_to_string(&mut buf)?;
        buf
    } else {
        args.to_string()
    };
    let raw = raw.trim();
    if raw.is_empty() {
        return Ok(json!({}));
    }
    serde_json::from_str(raw).with_context(|| format!("parse JSON arguments: {raw}"))
}

fn emit(v: &Value, compact: bool) -> Result<()> {
    if compact {
        println!("{}", serde_json::to_string(v)?);
    } else {
        println!("{}", serde_json::to_string_pretty(v)?);
    }
    Ok(())
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let mut mcp = Mcp::new(&cli)?;
    mcp.initialize().context("MCP initialize handshake")?;

    let result = match &cli.cmd {
        Cmd::Call { tool, args, file } => {
            let arguments = read_args(args, file)?;
            mcp.rpc("tools/call", json!({"name": tool, "arguments": arguments}))?
        }
        Cmd::List { full } => {
            let res = mcp.rpc("tools/list", json!({}))?;
            if *full {
                res
            } else {
                let tools = res
                    .get("tools")
                    .and_then(|t| t.as_array())
                    .cloned()
                    .unwrap_or_default();
                let slim: Vec<Value> = tools
                    .iter()
                    .map(|t| {
                        json!({
                            "name": t.get("name").cloned().unwrap_or(Value::Null),
                            "description": t.get("description").cloned().unwrap_or(Value::Null),
                        })
                    })
                    .collect();
                json!(slim)
            }
        }
        Cmd::Resources => mcp.rpc("resources/list", json!({}))?,
        Cmd::Resource { uri } => mcp.rpc("resources/read", json!({"uri": uri}))?,
        Cmd::Raw { method, params } => {
            let p = read_args(params, &None)?;
            mcp.rpc(method, p)?
        }
    };

    emit(&result, cli.compact)
}
