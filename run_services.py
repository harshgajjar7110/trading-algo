#!/usr/bin/env python3
"""
Unified service runner for Survivor Trading Strategy
Runs both backend (FastAPI/uvicorn) and frontend (Next.js) with colored output
"""

import subprocess
import sys
import os
import signal
import threading
import time
from pathlib import Path


# ANSI color codes for terminal output
class Colors:
    BACKEND = "\033[36m"      # Cyan
    FRONTEND = "\033[35m"     # Magenta
    INFO = "\033[32m"         # Green
    WARNING = "\033[33m"      # Yellow
    ERROR = "\033[31m"        # Red
    RESET = "\033[0m"         # Reset
    DIM = "\033[90m"          # Gray


class ServiceRunner:
    """Manages running backend and frontend services with colored output"""

    def __init__(self):
        self.processes = {}
        self.stop_event = threading.Event()
        self.workspace = Path(__file__).parent

    def check_backend_dependencies(self) -> bool:
        """Check if required backend dependencies are installed"""
        required = ["fastapi", "uvicorn", "pydantic_settings", "websockets", "yaml"]
        missing = []

        python = sys.executable
        venv_python = self.workspace / ".venv" / "bin" / "python"
        if not venv_python.exists():
            venv_python = self.workspace / ".venv" / "Scripts" / "python.exe"
        if venv_python.exists():
            python = str(venv_python)

        for module in required:
            try:
                result = subprocess.run(
                    [python, "-c", f"import {module.replace('pydantic_settings', 'pydantic_settings')}"],
                    capture_output=True,
                    timeout=5
                )
                if result.returncode != 0:
                    missing.append(module)
            except Exception:
                missing.append(module)

        if missing:
            self.log("SYSTEM", f"Missing backend dependencies: {', '.join(missing)}", "error")
            self.log("SYSTEM", "Please install: uv pip install -r web/backend/requirements.txt", "warning")
            return False
        return True

    def check_nodejs(self) -> bool:
        """Check if Node.js and npm are installed"""
        npm_cmd = "npm.cmd" if sys.platform == "win32" else "npm"

        try:
            result = subprocess.run(
                [npm_cmd, "--version"],
                capture_output=True,
                timeout=5
            )
            if result.returncode == 0:
                npm_version = result.stdout.decode().strip()
                self.log("SYSTEM", f"Found npm {npm_version}")
                return True
        except FileNotFoundError:
            pass
        except Exception:
            pass

        self.log("SYSTEM", "Node.js/npm not found!", "error")
        self.log("SYSTEM", "Please install Node.js from https://nodejs.org/", "warning")
        return False

    def check_frontend_dependencies(self) -> bool:
        """Check if frontend node_modules exist"""
        frontend_dir = self.workspace / "web" / "frontend"
        node_modules = frontend_dir / "node_modules"

        if not node_modules.exists():
            self.log("SYSTEM", "Frontend dependencies not installed!", "error")
            self.log("SYSTEM", f"Please run: cd web/frontend && npm install", "warning")
            return False
        return True

    def log(self, service: str, message: str, level: str = "info"):
        """Print formatted log message"""
        timestamp = time.strftime("%H:%M:%S")

        if service == "BACKEND":
            prefix_color = Colors.BACKEND
            label = "[BACKEND]"
        elif service == "FRONTEND":
            prefix_color = Colors.FRONTEND
            label = "[FRONTEND]"
        elif service == "SYSTEM":
            prefix_color = Colors.INFO
            label = "[SYSTEM]"
        else:
            prefix_color = Colors.RESET
            label = f"[{service}]"

        if level == "error":
            level_color = Colors.ERROR
        elif level == "warning":
            level_color = Colors.WARNING
        else:
            level_color = Colors.RESET

        print(f"{Colors.DIM}{timestamp}{Colors.RESET} "
              f"{prefix_color}{label:10}{Colors.RESET} "
              f"{level_color}{message}{Colors.RESET}", flush=True)

    def stream_output(self, process: subprocess.Popen, service: str, pipe):
        """Stream output from a process pipe with service label"""
        try:
            for line in iter(pipe.readline, b""):
                if self.stop_event.is_set():
                    break
                line = line.decode("utf-8", errors="replace").rstrip()
                if line:
                    self.log(service, line)
        except Exception as e:
            self.log(service, f"Stream error: {e}", "error")
        finally:
            pipe.close()

    def start_backend(self):
        """Start the FastAPI backend with uvicorn"""
        self.log("SYSTEM", "Starting backend service...")

        backend_dir = self.workspace / "web" / "backend"

        # Check if virtual environment exists
        venv_python = self.workspace / ".venv" / "Scripts" / "python.exe"
        if not venv_python.exists():
            venv_python = self.workspace / ".venv" / "bin" / "python"

        if venv_python.exists():
            cmd = [str(venv_python), "-m", "uvicorn", "app.main:app", "--reload", "--port", "8000"]
        else:
            cmd = [sys.executable, "-m", "uvicorn", "app.main:app", "--reload", "--port", "8000"]

        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"

        process = subprocess.Popen(
            cmd,
            cwd=backend_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=env
        )

        self.processes["backend"] = process

        # Start thread to stream output
        thread = threading.Thread(
            target=self.stream_output,
            args=(process, "BACKEND", process.stdout)
        )
        thread.daemon = True
        thread.start()

        self.log("SYSTEM", "Backend started on http://localhost:8000")
        return process

    def start_frontend(self):
        """Start the Next.js frontend"""
        self.log("SYSTEM", "Starting frontend service...")

        frontend_dir = self.workspace / "web" / "frontend"

        # Use npm.cmd on Windows, npm on Unix
        npm_cmd = "npm.cmd" if sys.platform == "win32" else "npm"

        env = os.environ.copy()
        env["FORCE_COLOR"] = "1"  # Enable colors in npm output
        env["NEXT_TELEMETRY_DISABLED"] = "1"  # Disable Next.js telemetry

        process = subprocess.Popen(
            [npm_cmd, "run", "dev"],
            cwd=frontend_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=env
        )

        self.processes["frontend"] = process

        # Start thread to stream output
        thread = threading.Thread(
            target=self.stream_output,
            args=(process, "FRONTEND", process.stdout)
        )
        thread.daemon = True
        thread.start()

        self.log("SYSTEM", "Frontend starting (will be available on http://localhost:3000)")
        return process

    def shutdown(self, signum=None, frame=None):
        """Gracefully shutdown all services"""
        self.log("SYSTEM", "Shutting down services...", "warning")
        self.stop_event.set()

        for name, process in self.processes.items():
            if process.poll() is None:  # Process is still running
                self.log("SYSTEM", f"Stopping {name}...")
                try:
                    if sys.platform == "win32":
                        process.terminate()
                        process.wait(timeout=5)
                    else:
                        process.send_signal(signal.SIGTERM)
                        process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self.log("SYSTEM", f"Force killing {name}...", "warning")
                    process.kill()

        self.log("SYSTEM", "All services stopped")
        sys.exit(0)

    def run(self):
        """Main entry point - start all services"""
        print(f"\n{Colors.INFO}{'='*60}{Colors.RESET}")
        print(f"{Colors.INFO}  Survivor Trading Strategy - Service Runner{Colors.RESET}")
        print(f"{Colors.INFO}{'='*60}{Colors.RESET}\n")

        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

        # Check all prerequisites
        if not self.check_backend_dependencies():
            self.log("SYSTEM", "Please install missing Python dependencies and try again", "error")
            return

        if not self.check_nodejs():
            self.log("SYSTEM", "Please install Node.js and try again", "error")
            return

        if not self.check_frontend_dependencies():
            self.log("SYSTEM", "Please install frontend dependencies and try again", "error")
            return

        try:
            # Start backend first
            backend = self.start_backend()

            # Wait for backend to initialize (give it up to 10 seconds)
            self.log("SYSTEM", "Waiting for backend to initialize...")
            backend_ready = False
            for _ in range(20):  # 20 * 0.5s = 10 seconds
                time.sleep(0.5)
                status = backend.poll()
                if status is not None:
                    self.log("BACKEND", f"Failed to start (exit code: {status})", "error")
                    self.shutdown()
                    return
                # Check if uvicorn is ready by looking at output (simplified check)
                backend_ready = True
                break

            if not backend_ready:
                self.log("BACKEND", "Failed to start within timeout", "error")
                self.shutdown()
                return

            # Start frontend
            frontend = self.start_frontend()

            print()
            self.log("SYSTEM", f"{Colors.INFO}All services started!{Colors.RESET}")
            self.log("SYSTEM", f"Backend:  {Colors.BACKEND}http://localhost:8000{Colors.RESET}")
            self.log("SYSTEM", f"Frontend: {Colors.FRONTEND}http://localhost:3000{Colors.RESET}")
            self.log("SYSTEM", f"API Docs: {Colors.BACKEND}http://localhost:8000/docs{Colors.RESET}")
            print()
            self.log("SYSTEM", "Press Ctrl+C to stop all services\n")

            # Monitor processes
            while True:
                backend_status = backend.poll()
                frontend_status = frontend.poll()

                if backend_status is not None:
                    self.log("BACKEND", f"Process exited with code {backend_status}", "error")
                    self.log("SYSTEM", "Backend crashed! Shutting down...", "warning")
                    break

                if frontend_status is not None:
                    self.log("FRONTEND", f"Process exited with code {frontend_status}", "error")
                    self.log("SYSTEM", "Frontend crashed! Shutting down...", "warning")
                    break

                time.sleep(0.5)

        except Exception as e:
            self.log("SYSTEM", f"Error: {e}", "error")
        finally:
            self.shutdown()


def main():
    """Entry point"""
    # Enable ANSI colors on Windows
    if sys.platform == "win32":
        try:
            import ctypes
            kernel32 = ctypes.windll.kernel32
            kernel32.SetConsoleMode(kernel32.GetStdHandle(-11), 7)
        except Exception:
            pass  # Fallback to no colors

    runner = ServiceRunner()
    runner.run()


if __name__ == "__main__":
    main()
