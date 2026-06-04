"""
tools/llm_client.py
Unified LLM client — supports Anthropic Claude, OpenAI, and Groq.
All nodes call llm_client.invoke(prompt) — provider is transparent.
Also handles B1 (Observability via Phoenix) and B2 (Cost Guardrails).
"""
import os
import sqlite3
import datetime
from loguru import logger
from config.settings import LLMConfig

DB_PATH = "metadata/data_ops.db"

# ── B2/B3/B4 SQLite Tables Setup ──────────────────────────────────────────────
def init_db():
    os.makedirs("metadata", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    # B2: Cost tracking table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_costs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id VARCHAR,
            recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            dataset VARCHAR,
            node VARCHAR,
            prompt_tokens INTEGER,
            completion_tokens INTEGER,
            cost_usd REAL
        )
    """)
    
    # B4: Concurrency lock table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS concurrency_locks (
            dataset VARCHAR,
            node VARCHAR,
            lock_status VARCHAR,
            acquired_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (dataset, node)
        )
    """)
    
    # B3: Human-in-the-loop approvals table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pending_approvals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id VARCHAR,
            dataset VARCHAR,
            node VARCHAR,
            sql_query TEXT,
            status VARCHAR DEFAULT 'PENDING',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()

init_db()

# ── B2 Cost Guardrail Checking ────────────────────────────────────────────────
def check_budget_guard():
    MAX_DAILY_BUDGET = 2.00  # Daily budget ceiling
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    today_str = datetime.datetime.now().strftime("%Y-%m-%d")
    cur.execute("SELECT SUM(cost_usd) FROM pipeline_costs WHERE date(recorded_at) = ?", (today_str,))
    total_cost = cur.fetchone()[0] or 0.0
    conn.close()
    
    if total_cost > MAX_DAILY_BUDGET:
        logger.error(f"Cost Guardrail | Daily budget exceeded! Current cost: ${total_cost:.4f} (Max: ${MAX_DAILY_BUDGET:.2f})")
        raise Exception(f"Cost Guardrail | Daily budget of ${MAX_DAILY_BUDGET:.2f} exceeded. Manual cost reset required.")

# ── B1 Observability (Arize Phoenix & OpenTelemetry) ──────────────────────────
_observability_initialized = False

def init_observability():
    global _observability_initialized
    if _observability_initialized:
        return
    try:
        from phoenix.otel import register
        from openinference.instrumentation.langchain import LangChainInstrumentor
        
        # Connect OpenTelemetry directly to the background Phoenix collector
        register(
            project_name="default",
            endpoint="http://localhost:6006/v1/traces"
        )
            
        # Instrument langchain to capture all traces automatically
        LangChainInstrumentor().instrument()
        logger.success("Observability | Arize Phoenix tracing pointing to http://localhost:6006")
        _observability_initialized = True
    except Exception as e:
        logger.warning(f"Observability | Failed to initialize Arize Phoenix tracing: {e}")


class LLMClient:
    """
    Singleton LLM client. Reads LLM_PROVIDER from .env to pick backend.
    Supported: 'anthropic' (default), 'openai', 'groq'
    """

    def __init__(self):
        self.provider = LLMConfig.PROVIDER
        self.model    = LLMConfig.MODEL
        self._client  = None
        init_observability()
        self._init_client()

    def _init_client(self):
        if self.provider == "anthropic":
            try:
                import anthropic
                self._client = anthropic.Anthropic(api_key=LLMConfig.API_KEY)
                logger.info(f"LLM | Anthropic ready | model={self.model}")
            except ImportError:
                logger.warning("LLM | anthropic not installed — falling back to openai")
                self.provider = "openai"
                self._init_openai()

        elif self.provider == "groq":
            try:
                from langchain_groq import ChatGroq
                self._client = ChatGroq(
                    api_key=os.getenv("GROQ_API_KEY"),
                    model_name=self.model or "llama-3.1-8b-instant",
                )
                logger.info(f"LLM | Groq ready | model={self.model}")
            except ImportError:
                logger.warning("LLM | groq not installed — falling back to openai")
                self.provider = "openai"
                self._init_openai()

        else:
            self._init_openai()

    def _init_openai(self):
        try:
            from langchain_openai import ChatOpenAI
            self._client = ChatOpenAI(
                api_key=LLMConfig.API_KEY,
                model=self.model or "gpt-4o",
            )
            logger.info(f"LLM | OpenAI ready | model={self.model}")
        except ImportError:
            logger.error("LLM | No LLM package found. Install anthropic, langchain-openai, or langchain-groq.")
            raise

    def invoke(self, prompt: str) -> str:
        """Single-turn LLM call. Returns plain string response."""
        check_budget_guard()
        
        try:
            prompt_tokens = 0
            completion_tokens = 0
            
            if self.provider == "anthropic":
                import anthropic
                response = self._client.messages.create(
                    model=self.model,
                    max_tokens=1024,
                    messages=[{"role": "user", "content": prompt}],
                )
                text_response = response.content[0].text.strip()
                prompt_tokens = response.usage.input_tokens
                completion_tokens = response.usage.output_tokens
            else:
                response = self._client.invoke(prompt)
                text_response = response.content.strip()
                usage = getattr(response, "response_metadata", {}).get("token_usage", {})
                prompt_tokens = usage.get("prompt_tokens", 0)
                completion_tokens = usage.get("completion_tokens", 0)
                
            # Fallback estimation if token stats are missing/zero
            if not prompt_tokens:
                prompt_tokens = len(prompt) // 4
            if not completion_tokens:
                completion_tokens = len(text_response) // 4
                
            # Calculate cost (using Groq / Llama 3 70B rates: $0.59/M input, $0.79/M output)
            cost_input = prompt_tokens * 0.00000059
            cost_output = completion_tokens * 0.00000079
            cost_usd = cost_input + cost_output
            
            # Extract context logs
            run_id = os.environ.get("PIPELINE_RUN_ID", "local_run")
            dataset = os.environ.get("ACTIVE_DATASET", "customers")
            node = os.environ.get("CURRENT_NODE", "unknown")
            
            # Save to SQLite
            conn = sqlite3.connect(DB_PATH)
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO pipeline_costs (run_id, dataset, node, prompt_tokens, completion_tokens, cost_usd)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (run_id, dataset, node, prompt_tokens, completion_tokens, cost_usd))
            conn.commit()
            conn.close()
            
            return text_response
            
        except Exception as e:
            logger.error(f"LLM | invoke failed: {e}")
            raise


# Singleton — imported and reused by all nodes
llm_client = LLMClient()
