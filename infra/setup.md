# Local Development Setup

Follow these steps to prepare the local development environment for this project. This configuration ensures that your IDE features—such as code autocomplete, Jinja-SQL compilation, and interactive lineage graphs—work perfectly before we deploy any Docker containers.

---

## Table of Contents

* [1. Install Base Software](#1-install-base-software)
* [2. Install Required VS Code Extensions](#2-install-required-vs-code-extensions)
* [3. Clone the Repository](#3-clone-the-repository)
* [4. Configure Python Virtual Environment](#4-configure-python-virtual-environment)
* [5. Install dbt and Verify Setup](#5-install-dbt-and-verify-setup)
* [6. Prepare Environment Variables and Secrets](#6-prepare-environment-variables-and-secrets)
* [7. AI Assistant Setup with Continue and Ollama (Optional)](#7-ai-assistant-setup-with-continue-and-ollama-optional)

---

### 1. Install Base Software

#### Visual Studio Code

Download and install the editor from the official website:

* [Download VS Code](https://code.visualstudio.com/)

#### Docker Desktop

Download and install the Docker engine backend. Ensure that Docker Desktop is running in the background:

* [Download Docker Desktop](https://www.docker.com/products/docker-desktop/)

> 💡 **Note:** The project relies on Docker to orchestrate heavy services like Apache Airflow, dbt runtime transformations, and the ClickHouse OLAP database.

---

### 2. Install Required VS Code Extensions

Open VS Code, navigate to the Extensions tab (`Ctrl+Shift+X` or `Cmd+Shift+X`), and install the following toolset:

* **Python** (by Microsoft) — Enables IntelliSense, linting, and structural integration with your virtual environments.
* **dbt Power User** — Essential for on-the-fly Jinja-SQL compilation, query previews, and interactive lineage graphs directly inside the IDE.
* **Docker** (by Microsoft) — Provides a clean GUI interface to manage containers, view application logs, and restart services without using the terminal.
* **Continue** — A powerful interface bridge to connect local or cloud-based AI code assistants (like Ollama + Qwen2.5-Coder). Optional — see [Section 7](#7-ai-assistant-setup-with-continue-and-ollama-optional).

---

### 3. Clone the Repository

Open your system terminal, clone the repository, and step into the project's root directory:

```bash
git clone https://github.com/<your-username>/game-market-analytics.git
cd game-market-analytics
```

---

### 4. Configure Python Virtual Environment

#### Check Python Version

Before creating the environment, verify that Python 3.12 is installed on your machine:

```bash
# On Windows (using the Python Launcher)
py --version

# On macOS / Linux
python3 --version
```

> ⚠️ **Important (Windows):** If your terminal states that Python is not found,
> download Python 3.12 directly from [Python.org](https://www.python.org/downloads/release/python-3120/).
> Run the installer and **strictly check the box that says "Add python.exe to PATH"**
> on the very first screen.

#### Create the Environment

Run the following command inside the root `game-market-analytics` directory to generate a hidden, isolated folder named `.venv`:

```bash
# On Windows
py -3.12 -m venv .venv

# On macOS / Linux
python3.12 -m venv .venv
```

#### Select Python Interpreter in VS Code

Tell VS Code to link your workspace with the newly created virtual environment:

1. Open the Command Palette using `Ctrl+Shift+P` (`Cmd+Shift+P` on Mac).
2. Type and select **Python: Select Interpreter**.
3. Choose the option pointing to your local workspace environment: `.\.venv\Scripts\python.exe` (labeled as **Recommended**).

> 💡 **Note:** You do not need to manually activate the virtual environment via terminal commands. VS Code will handle this automatically once the interpreter is selected.

---

### 5. Install dbt and Verify Setup

1. Kill your current terminal instance by clicking the **Trash Can icon** in the terminal panel to completely reset the session.
2. Open a fresh terminal tab (`` Ctrl+` ``). You will immediately see the **`(.venv)`** prefix in your terminal prompt, confirming the environment is active.
3. Install the dbt core framework along with the ClickHouse database adapter:

```bash
pip install dbt-core==1.8.0 dbt-clickhouse==1.8.5
pip freeze > requirements.txt
```

4. Verify the installation:

```bash
dbt --version
```

Expected output:

```
Core:
  - installed: 1.8.0
Plugins:
  - clickhouse: 1.8.5
```

---

### 6. Prepare Environment Variables and Secrets

Before starting Docker containers, you need to configure environment variables and generate secrets. All sensitive values are stored in a local `.env` file that is never committed to the repository.

#### Step 1: Create the `.env` File

Copy the provided template:

```bash
# On Windows
copy infra\.env.example infra\.env

# On macOS / Linux
cp infra/.env.example infra/.env
```

Open `infra/.env` and fill in all values as described below.

#### Step 2: Generate the Airflow Fernet Key

Airflow uses a Fernet key to encrypt sensitive data stored in its metadata database — Connections, Variables, and passwords. Without it, Airflow will fail to start.

Generate the key once and save it. It must remain the same across all container restarts — if it changes, previously encrypted data becomes unreadable.

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Copy the output and set it in `infra/.env`:

```bash
AIRFLOW__CORE__FERNET_KEY=your_generated_key_here
```

> ⚠️ **Never regenerate this key** after the first run. If lost, all encrypted Airflow data must be re-entered manually.

#### Step 3: Review `.env` Values

Your final `infra/.env` should look like this:

```bash
# Airflow
AIRFLOW_UID=50000
AIRFLOW__CORE__FERNET_KEY=your_generated_fernet_key
_AIRFLOW_WWW_USER_USERNAME=your_airflow_username
_AIRFLOW_WWW_USER_PASSWORD=your_airflow_password

# PostgreSQL — Airflow metadata database
POSTGRES_USER=your_postgres_user
POSTGRES_PASSWORD=your_postgres_password
POSTGRES_DB=airflow

# ClickHouse — analytics DWH
CLICKHOUSE_DB=analytics

# airflow_user — used by dbt and DAGs (RW on analytics.*)
CLICKHOUSE_AIRFLOW_USER=airflow_user
CLICKHOUSE_AIRFLOW_PASSWORD=your_airflow_password

# admin — for init.sh and manual operations (root)
# MUST match the password used in users.d/01-admin.xml
CLICKHOUSE_ADMIN_USER=admin
CLICKHOUSE_ADMIN_PASSWORD=your_admin_password
```

> 💡 **Note:** The `.env` file is listed in `.gitignore` and will never be committed to the repository. Only `.env.example` (with placeholder values) is tracked by Git.

#### Step 4: Create the ClickHouse Admin User

```bash
# Copy template
cp infra/clickhouse/users.d/01-admin.xml.example infra/clickhouse/users.d/01-admin.xml

# Generate hash
HASH=$(echo -n "your_admin_password" | sha256sum | awk '{print $1}')

# Replace placeholder
sed -i "s|REPLACE_WITH_SHA256_OF_YOUR_PASSWORD|$HASH|" infra/clickhouse/users.d/01-admin.xml
```
> 💡 **Note:** The `01-admin.xml` file is listed in `.gitignore` and will never be committed to the repository. Only `01-admin.xml.example` (with placeholder values) is tracked by Git.

#### Step 5: Rotating the Admin Password (if needed)

```bash
NEW_HASH=$(echo -n "new_password" | sha256sum | awk '{print $1}')
sed -i "s|<password_sha256_hex>.*</password_sha256_hex>|<password_sha256_hex>$NEW_HASH</password_sha256_hex>|" infra/clickhouse/users.d/01-admin.xml
sed -i "s|CLICKHOUSE_ADMIN_PASSWORD=.*|CLICKHOUSE_ADMIN_PASSWORD=new_password|" infra/.env
docker compose restart clickhouse
```

> 💡 **Note:** The `airflow_user` password is managed via Airflow UI → Admin → Connections (clickhouse_default).
----

### 7. AI Assistant Setup with Continue and Ollama (Optional)

Continue is an open-source AI coding assistant that connects VS Code to local models via Ollama. It enables context-aware code generation, inline completions, and codebase-wide semantic search — all running locally without sending data to external servers.

> 💡 **Note:** This section is optional. The project works without an AI assistant. Continue is recommended for accelerating dbt model generation, DAG authoring, and SQL optimization.

#### Step 1: Install Ollama

Ollama is a local runtime for AI models.

**Windows:** Download and run the installer: [Download Ollama](https://ollama.ai/)

**macOS / Linux:**

```bash
curl -fsSL https://ollama.ai/install.sh | sh
```

Verify installation:

```bash
ollama --version
```

#### Step 2: Start Ollama Service

Ollama must be running before pulling models or using Continue.

```bash
ollama serve
```

**Windows:** Ollama runs as a system service automatically after installation — no manual start needed.

**Linux:**

```bash
sudo systemctl start ollama
```

Verify it is running:

```bash
curl http://localhost:11434/api/tags
```

#### Step 3: Pull Models Locally

Pull the models required for this project:

```bash
# Chat and code generation (requires ~20GB VRAM)
ollama pull qwen2.5-coder:32b

# Lightweight alternative (requires ~8GB VRAM)
ollama pull qwen2.5-coder:7b

# Autocomplete model (fast, minimal VRAM)
ollama pull qwen2.5-coder:1.5b-base

# Embedding model for codebase indexing
ollama pull nomic-embed-text
```

Verify all models are available:

```bash
ollama list
```

#### Step 4: Create the Continue Configuration File

Continue reads its configuration from `C:\Users\<username>\.continue\config.yaml` (Windows) or `~/.continue/config.yaml` (macOS/Linux). This is a global file that applies across all projects.

Open it through the Continue UI:

1. Open the **Continue** sidebar in VS Code
2. Click the **Settings** icon (⚙️) in the Continue panel
3. Click **Edit config.yaml**

Replace the contents with the following configuration:

```yaml
name: Main Config
version: 1.0.0
schema: v1

models:
  - name: qwen2.5-coder:32b
    provider: ollama
    model: qwen2.5-coder:32b
    roles:
      - chat
      - edit
      - apply

  - name: qwen2.5-coder:7b
    provider: ollama
    model: qwen2.5-coder:7b
    roles:
      - chat
      - edit
      - apply

  - name: qwen2.5-coder:1.5b
    provider: ollama
    model: qwen2.5-coder:1.5b-base
    roles:
      - autocomplete

  - name: Nomic Embed
    provider: ollama
    model: nomic-embed-text:latest
    roles:
      - embed

context:
  - provider: codebase
```

**Key configuration fields:**

| Field | Purpose |
|---|---|
| `provider` | `ollama` for local models |
| `roles` | `chat` / `edit` / `apply` — code generation; `autocomplete` — inline suggestions; `embed` — codebase indexing |
| `context: codebase` | Enables `@codebase` semantic search across the entire project |

#### Step 5: Configure Project Rules

Project-specific instructions for the AI assistant are stored in `.github/instructions/analytics-engineer.instructions.md`. This file is committed to the repository and automatically loaded by Continue when working in this project.

It defines the tech stack, data layer conventions, and coding rules — so the model generates ClickHouse-compatible SQL, follows the `staging → core → marts` dbt structure, and uses the TaskFlow API for Airflow DAGs without requiring manual context in every message.

No additional setup is required — Continue picks up the file automatically.

#### Step 6: Verify Codebase Indexing

Continue indexes the project files using the embedding model to enable semantic search.

1. Open the Continue sidebar
2. Click the **Settings** icon → **Indexing**
3. Confirm **Indexing complete** is shown with no errors
4. If errors appear, click **Click to re-index**

Once indexed, use `@codebase` in the chat to search across the entire project.