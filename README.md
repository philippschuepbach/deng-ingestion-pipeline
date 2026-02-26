# deng-ingestion-pipeline

## 🛠️ Prerequisites & Setup

This project is built using a modern Data Engineering stack. To ensure reproducibility and performance, follow the setup instructions below.

### 1. Core Requirements
* **WSL2 (Ubuntu 22.04+):** It is highly recommended to clone this repository directly into the Linux filesystem (`~/`) for optimal performance. Do **not** run this from `/mnt/c/` to avoid I/O lag and permission issues.
* **uv:** An extremely fast Python package and project manager.
  * Install via: `curl -LsSf https://astral.sh/uv/install.sh | sh`
* **Docker Desktop:** Ensure "WSL Integration" is enabled in settings for your specific Ubuntu distribution.

### 2. Recommended VS Code Extensions
For a seamless development experience, install the following:
* **WSL** (`ms-vscode-remote.remote-wsl`) – Run VS Code natively inside Linux.
* **Python** (`ms-python.python`) – IntelliSense and environment management.
* **Black Formatter** (`ms-python.black-formatter`) – Auto-formatting on save.

### 3. Local Installation
Once the tools above are installed, run the following commands in your WSL terminal:

1. **Sync the environment:** `uv sync`
2. **Install pre-commit hooks:** `uv run pre-commit install`
3. **Verify Installation:** `uv run pre-commit run --all-files`
