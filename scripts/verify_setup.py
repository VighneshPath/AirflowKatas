#!/usr/bin/env python3
"""
Setup Verification Script

This script verifies that the Airflow development environment is properly configured
and all required components are running correctly.

Usage:
    python scripts/verify_setup.py
"""

import subprocess
import sys
import time
import os
from typing import Dict, List, Tuple


def run_command(command: str) -> Tuple[bool, str]:
    """Run a shell command and return success status and output."""
    try:
        result = subprocess.run(
            command.split(),
            capture_output=True,
            text=True,
            timeout=30
        )
        return result.returncode == 0, result.stdout + result.stderr
    except subprocess.TimeoutExpired:
        return False, "Command timed out"
    except Exception as e:
        return False, str(e)


def check_docker() -> Dict[str, any]:
    """Check if Docker is installed and running."""
    print("Checking Docker installation...")

    # Check Docker version
    success, output = run_command("docker --version")
    if not success:
        return {
            "status": "failed",
            "message": "Docker is not installed or not in PATH",
            "details": output
        }

    # Check if Docker daemon is running
    success, output = run_command("docker info")
    if not success:
        return {
            "status": "failed",
            "message": "Docker daemon is not running",
            "details": "Start Docker Desktop or run 'sudo systemctl start docker'"
        }

    return {
        "status": "passed",
        "message": "Docker is installed and running",
        "details": output.split('\n')[0]  # First line with version
    }


def check_docker_compose() -> Dict[str, any]:
    """Check if Docker Compose is available."""
    print("Checking Docker Compose...")

    success, output = run_command("docker-compose --version")
    if not success:
        # Try docker compose (newer syntax)
        success, output = run_command("docker compose version")
        if not success:
            return {
                "status": "failed",
                "message": "Docker Compose is not installed",
                "details": "Install Docker Compose or use Docker Desktop"
            }

    return {
        "status": "passed",
        "message": "Docker Compose is available",
        "details": output.strip()
    }


def check_airflow_containers() -> Dict[str, any]:
    """Check if Airflow containers are running."""
    print("Checking Airflow containers...")

    success, output = run_command("docker-compose ps")
    if not success:
        return {
            "status": "failed",
            "message": "Could not check container status",
            "details": "Run 'docker-compose up -d' to start Airflow"
        }

    # Check for required services
    required_services = ["webserver", "scheduler", "postgres", "redis"]
    running_services = []

    for line in output.split('\n'):
        if any(service in line.lower() for service in required_services):
            if "up" in line.lower():
                running_services.append(line.split()[0])

    if len(running_services) < len(required_services):
        return {
            "status": "warning",
            "message": f"Only {len(running_services)} of {len(required_services)} services running",
            "details": f"Running: {', '.join(running_services)}"
        }

    return {
        "status": "passed",
        "message": "All Airflow containers are running",
        "details": f"Services: {', '.join(running_services)}"
    }


def check_airflow_webserver() -> Dict[str, any]:
    """Check if Airflow webserver is accessible."""
    print("Checking Airflow webserver...")

    try:
        # Try to import requests, if not available, skip this check
        import requests

        response = requests.get("http://localhost:8080/health", timeout=10)
        if response.status_code == 200:
            return {
                "status": "passed",
                "message": "Airflow webserver is accessible",
                "details": "http://localhost:8080 is responding"
            }
        else:
            return {
                "status": "warning",
                "message": f"Webserver responded with status {response.status_code}",
                "details": "Check if Airflow is fully initialized"
            }
    except ImportError:
        return {
            "status": "warning",
            "message": "Cannot check webserver (requests package not available)",
            "details": "Install requests: pip install requests"
        }
    except Exception as e:
        return {
            "status": "failed",
            "message": "Error checking webserver",
            "details": str(e)
        }


def check_dag_folder() -> Dict[str, any]:
    """Check if DAG folder exists and contains files."""
    print("Checking DAG folder...")

    if not os.path.exists("dags"):
        return {
            "status": "failed",
            "message": "DAGs folder does not exist",
            "details": "Create 'dags' directory in project root"
        }

    dag_files = [f for f in os.listdir("dags") if f.endswith('.py')]

    if not dag_files:
        return {
            "status": "warning",
            "message": "No DAG files found",
            "details": "Add Python files to the 'dags' directory"
        }

    return {
        "status": "passed",
        "message": f"Found {len(dag_files)} DAG files",
        "details": f"Files: {', '.join(dag_files[:3])}{'...' if len(dag_files) > 3 else ''}"
    }


def check_module_structure() -> Dict[str, any]:
    """Check if module structure exists."""
    print("Checking module structure...")

    if not os.path.exists("modules"):
        return {
            "status": "failed",
            "message": "Modules directory does not exist",
            "details": "Module structure is missing"
        }

    module_dirs = [d for d in os.listdir(
        "modules") if os.path.isdir(os.path.join("modules", d))]

    if not module_dirs:
        return {
            "status": "warning",
            "message": "No module directories found",
            "details": "Module content may be missing"
        }

    return {
        "status": "passed",
        "message": f"Found {len(module_dirs)} learning modules",
        "details": f"Modules: {', '.join(sorted(module_dirs)[:3])}{'...' if len(module_dirs) > 3 else ''}"
    }


def check_python_dependencies() -> Dict[str, any]:
    """Check if required Python packages are available."""
    print("Checking Python dependencies...")

    required_packages = ["requests"]
    missing_packages = []

    for package in required_packages:
        try:
            __import__(package)
        except ImportError:
            missing_packages.append(package)

    if missing_packages:
        return {
            "status": "warning",
            "message": f"Missing Python packages: {', '.join(missing_packages)}",
            "details": f"Install with: pip install {' '.join(missing_packages)}"
        }

    return {
        "status": "passed",
        "message": "All required Python packages are available",
        "details": "Dependencies satisfied"
    }


def main():
    """Run all verification checks."""
    print("🔍 Verifying Airflow Development Environment Setup\n")

    checks = [
        ("Docker Installation", check_docker),
        ("Docker Compose", check_docker_compose),
        ("Python Dependencies", check_python_dependencies),
        ("DAG Folder", check_dag_folder),
        ("Module Structure", check_module_structure),
        ("Airflow Containers", check_airflow_containers),
        ("Airflow Webserver", check_airflow_webserver),
    ]

    results = []

    for check_name, check_function in checks:
        result = check_function()
        result["check"] = check_name
        results.append(result)

        # Print immediate feedback
        status_icon = {
            "passed": "✅",
            "warning": "⚠️",
            "failed": "❌"
        }.get(result["status"], "❓")

        print(f"{status_icon} {check_name}: {result['message']}")
        if result.get("details"):
            print(f"   {result['details']}")
        print()

    # Summary
    passed = sum(1 for r in results if r["status"] == "passed")
    warnings = sum(1 for r in results if r["status"] == "warning")
    failed = sum(1 for r in results if r["status"] == "failed")

    print("=" * 60)
    print("VERIFICATION SUMMARY")
    print("=" * 60)
    print(f"✅ Passed: {passed}")
    print(f"⚠️  Warnings: {warnings}")
    print(f"❌ Failed: {failed}")
    print()

    if failed > 0:
        print("❌ Setup verification failed. Please address the failed checks above.")
        print("\nCommon solutions:")
        print("• Start Docker: docker-compose up -d")
        print("• Check port 8080: lsof -ti:8080")
        print("• Wait for initialization: Airflow takes 2-3 minutes to start")
        sys.exit(1)
    elif warnings > 0:
        print("⚠️  Setup verification completed with warnings.")
        print("Your environment should work, but consider addressing the warnings.")
    else:
        print("🎉 All checks passed! Your Airflow environment is ready.")
        print("\nNext steps:")
        print("• Open http://localhost:8080 in your browser")
        print("• Login with username: airflow, password: airflow")
        print("• Start working on Module 1 exercises")


if __name__ == "__main__":
    main()
