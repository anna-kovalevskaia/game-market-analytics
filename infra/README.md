# Local Development Setup

Follow these steps to prepare the local development environment for this project. This configuration ensures that your IDE features—such as code autocomplete, Jinja-SQL compilation, and interactive lineage graphs—work perfectly before we deploy any Docker containers.

---

## Table of Contents

* [1. Install Base Software](#1-install-base-software)
* [2. Install Required VS Code Extensions](#2-install-required-vs-code-extensions)
* [3. Clone the Repository](#3-clone-the-repository)
* [4. Configure Python Virtual Environment](#4-configure-python-virtual-environment)
* [5. Install dbt and Verify Setup](#5-install-dbt-and-verify-setup)
* [6. AI Assistant Setup with Continue and Ollama](#6-ai-assistant-setup-with-continue-and-ollama)

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

### Why Continue?

Continue was chosen over VS Code's built-in "Open in Agents" because local models do not work correctly in Agents and Plan modes with the built-in tool—they only return raw JSON without proper agentic behavior.

### Step 1: Install Ollama

Ollama is a local runtime for AI models. Download and install it:

* [Download Ollama](https://ollama.ai/)

On macOS/Linux:
```bash
curl -fsSL https://ollama.ai/install.sh | sh
```

On Windows, download the installer and run it. Verify installation:

```bash
ollama --version
```

### Step 2: Pull Models Locally

Pull the models you want to use locally:

```bash
# Large local model (32B parameters, requires ~20GB VRAM)
ollama pull qwen2.5-coder:32b

# Smaller local model (7B parameters, requires ~8GB VRAM)
ollama pull qwen2.5-coder:7b

# Alternative local model
ollama pull deepseek-coder
```

To verify models are available:

```bash
ollama list
```

### Step 3: Create the Continue Configuration File

Continue stores configuration in a `.continue/config.yml` file. This file is created through the Continue UI:

1. After installing Continue, open the **Continue** sidebar in VS Code
2. Click on the **Settings** icon (⚙️) in the Continue panel
3. Click **Edit config.yml**
4. Add your Ollama models to the `models` section

**Example config.yml:**

```yaml
name: Main Config
version: 1.0.0
schema: v1

models:
  - name: deepseek-coder:33b
    provider: ollama
    model: deepseek-coder:33b
    roles:
      - chat
      - edit
      - apply

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

**Key Configuration Fields:**

| Field | Purpose |
|---|---|
| `name` | Display name for the model in Continue UI |
| `provider` | AI provider (ollama for local/cloud) |
| `model` | Model identifier (must match `ollama list` output) |
| `roles` | What the model can do: `chat` (conversations), `edit` (code generation), `apply` (apply edits), `autocomplete` (inline suggestions), `embed` (codebase indexing) |
| `context` | Data sources for context (codebase enables intelligent code search) |

### Step 4: Configure Rules

Rules define coding standards that guide the AI assistant. The project includes `.continue/rules/analytics-engineer.md` with project-specific guidelines:

**File Location:** `.continue/rules/analytics-engineer.md`

Rules are automatically loaded by Continue when you open the project. You can view and edit them through the Continue UI or directly in the editor.

### Step 5: Embedding Model for Codebase Indexing

The embedding model (role: `embed`) enables intelligent semantic search within your codebase. In the config.yml example above, it's configured as:

```yaml
  - name: Nomic Embed
    provider: ollama
    model: nomic-embed-text:latest
    roles:
      - embed
```

First, pull the embedding model:

```bash
ollama pull nomic-embed-text
```

This allows Continue to index and understand your project structure for better context in code generation.

### Step 6: Start Ollama Service

Keep Ollama running in the background. Start it with:

```bash
ollama serve
```

Or, for persistent background service:

**On macOS:**
```bash
brew services start ollama
```

**On Windows:**
Ollama runs as a system service by default after installation.

**On Linux:**
```bash
sudo systemctl start ollama
```

Verify Ollama is running:

```bash
curl http://localhost:11434/api/tags
```

### Step 7: Open Project in Continue

1. Install **Continue** extension from VS Code Marketplace
2. Open the project in VS Code
3. Continue will automatically detect `.continue/config.yml` and load models
4. Click the **Continue** icon in the sidebar to open the chat interface
5. Select a model from the dropdown and start coding

### Using Continue

**Available Commands:**

- `/chat` — Ask the AI assistant a question (default mode)
- `/edit` — Generate or modify code in the current file
- `/codebase` — Ask questions about your entire codebase
- `/docs` — Ask about documentation
- `Ctrl+I` (Windows/Linux) or `Cmd+I` (Mac) — Quick code generation inline

**Example Usage:**

In VS Code, type:
```
/edit Generate a dbt model for staging raw Steam API data
```

Continue will generate code based on your project context, rules, and codebase structure.
