# Local Development Setup

Follow these steps to prepare the local development environment for this project. This configuration ensures that your IDE features—such as code autocomplete, Jinja-SQL compilation, and interactive lineage graphs—work perfectly before we deploy any Docker containers.

---

## Table of Contents

* [1. Install Base Software](#1-install-base-software)
* [2. Install Required VS Code Extensions](#2-install-required-vs-code-extensions)
* [3. Clone the Repository](#3-clone-the-repository)
* [4. Configure Python Virtual Environment](#4-configure-python-virtual-environment)
* [5. Install dbt and Verify Setup](#5-install-dbt-and-verify-setup)
* [6. AI Assistant Setup with Continue and Ollama (Optional)](#6-ai-assistant-setup-with-continue-and-ollama)

---

## 1. Install Base Software

### Visual Studio Code

Download and install the editor from the official website:

* [Download VS Code](https://code.visualstudio.com/)

### Docker Desktop

Download and install the Docker engine backend. Ensure that Docker Desktop is running in the background:

* [Download Docker Desktop](https://www.docker.com/products/docker-desktop/)

> 💡 **Note:** The project relies on Docker to orchestrate heavy services like Apache Airflow, dbt runtime transformations, and the ClickHouse OLAP database.

---

## 2. Install Required VS Code Extensions

Open VS Code, navigate to the Extensions tab (`Ctrl+Shift+X` or `Cmd+Shift+X`), and install the following toolset:

* **Python** (by Microsoft) — Enables IntelliSense, linting, and structural integration with your virtual environments.
* **dbt Power User** — Essential for on-the-fly Jinja-SQL compilation, query previews, and interactive lineage graphs directly inside the IDE.
* **Docker** (by Microsoft) — Provides a clean GUI interface to manage containers, view application logs, and restart services without using the terminal.
* **Continue** — A powerful interface bridge to connect local or cloud-based AI code assistants (like Ollama + Qwen2.5-Coder).

---

## 3. Clone the Repository

Open your system terminal, clone the repository, and step into the project's root directory:

```bash
git clone https://github.com/<your-username>/game-market-analytics.git
cd game-market-analytics
```

---

## 4. Configure Python Virtual Environment

### Check Python Version

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

### Create the Environment

Run the following command inside the root `game-market-analytics` directory to generate a hidden, isolated folder named `.venv`:

```bash
# On Windows
py -3.12 -m venv .venv

# On macOS / Linux
python3.12 -m venv .venv
```

### Select Python Interpreter in VS Code

Tell VS Code to link your workspace with the newly created virtual environment:

1. Open the Command Palette using `Ctrl+Shift+P` (`Cmd+Shift+P` on Mac).
2. Type and select **Python: Select Interpreter**.
3. Choose the option pointing to your local workspace environment: `.\.venv\Scripts\python.exe` (labeled as **Recommended**).

> 💡 **Note:** You do not need to manually activate the virtual environment via terminal commands. VS Code will handle this automatically once the interpreter is selected.

---

## 5. Install dbt and Verify Setup

1. Kill your current terminal instance by clicking the **Trash Can icon** in the terminal panel to completely reset the session.
2. Open a fresh terminal tab (`Ctrl + ` `). You will immediately see the **`(.venv)`** prefix in your terminal prompt, confirming the environment is active.
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

## 6. AI Assistant Setup with Continue and Ollama

Continue is an open-source AI coding assistant that connects VS Code to local models via Ollama. It enables context-aware code generation, inline completions, and codebase-wide semantic search — all running locally without sending data to external servers.

> 💡 **Note:** This section is optional. The project works without an AI assistant. Continue is recommended for accelerating dbt model generation, DAG authoring, and SQL optimization.

---

### Step 1: Install Ollama

Ollama is a local runtime for AI models.

**Windows:**
Download and run the installer: [Download Ollama](https://ollama.ai/)

**macOS / Linux:**
```bash
curl -fsSL https://ollama.ai/install.sh | sh
```

Verify installation:
```bash
ollama --version
```

---

### Step 2: Start Ollama Service

Ollama must be running before pulling models or using Continue.

```bash
ollama serve
```

On Windows, Ollama runs as a system service automatically after installation — no manual start needed.

On Linux:

```bash
sudo systemctl start ollama
```

Verify it is running:

```bash
curl http://localhost:11434/api/tags
```

---

### Step 3: Pull Models Locally

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

---

### Step 4: Create the Continue Configuration File

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

---

### Step 5: Configure Project Rules

Project-specific instructions for the AI assistant are stored in `.github/instructions/analytics-engineer.instructions.md`. This file is committed to the repository and automatically loaded by Continue when working in this project.

It defines the tech stack, data layer conventions, and coding rules — so the model generates ClickHouse-compatible SQL, follows the `staging → core → marts` dbt structure, and uses the TaskFlow API for Airflow DAGs without requiring manual context in every message.

No additional setup is required — Continue picks up the file automatically.

---

### Step 6: Verify Codebase Indexing

Continue indexes the project files using the embedding model to enable semantic search.

1. Open the Continue sidebar
2. Click the **Settings** icon → **Indexing**
3. Confirm **Indexing complete** is shown with no errors
4. If errors appear, click **Click to re-index**

Once indexed, use `@Codebase` in the chat to search across the entire project:

```
@Codebase what models exist in the staging layer?
```
