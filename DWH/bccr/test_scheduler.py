#!/usr/bin/env python3
"""
Test script for BCCR Scheduler
Verifies that all components work correctly
"""
import subprocess
import sys
import time
from pathlib import Path

def run_command(cmd, description):
    """Run a command and report results"""
    print(f"\n{'='*60}")
    print(f"Testing: {description}")
    print(f"Command: {' '.join(cmd)}")
    print(f"{'='*60}")
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        print(result.stdout)
        if result.stderr:
            print(f"STDERR: {result.stderr}")
        return result.returncode == 0
    except subprocess.TimeoutExpired:
        print(f"❌ TIMEOUT: Command took too long")
        return False
    except Exception as e:
        print(f"❌ ERROR: {str(e)}")
        return False

def test_docker_installation():
    """Test if Docker is installed"""
    return run_command(
        ["docker", "--version"],
        "Docker installation"
    )

def test_docker_compose():
    """Test if Docker Compose is installed"""
    return run_command(
        ["docker-compose", "--version"],
        "Docker Compose installation"
    )

def test_python_packages():
    """Test if required Python packages are available"""
    packages = ["schedule", "requests", "pyodbc", "python-dotenv"]
    all_good = True
    
    for pkg in packages:
        result = run_command(
            [sys.executable, "-m", "pip", "show", pkg],
            f"Python package: {pkg}"
        )
        if not result:
            all_good = False
    
    return all_good

def test_env_file():
    """Test if .env file exists and has required variables"""
    env_file = Path(__file__).parent.parent.parent / ".env"
    
    print(f"\n{'='*60}")
    print(f"Testing: .env file")
    print(f"{'='*60}")
    
    if not env_file.exists():
        print(f"❌ .env file not found at {env_file}")
        return False
    
    print(f"✓ .env file found at {env_file}")
    
    required_vars = [
        "BCCR_USER",
        "BCCR_PASSWORD",
        "serverenv",
        "databaseenv",
        "usernameenv",
        "passwordenv"
    ]
    
    with open(env_file) as f:
        content = f.read()
    
    missing = []
    for var in required_vars:
        if f"{var}=" not in content:
            missing.append(var)
    
    if missing:
        print(f"❌ Missing variables: {', '.join(missing)}")
        return False
    
    print(f"✓ All required variables present")
    return True

def main():
    """Run all tests"""
    print("""
╔════════════════════════════════════════════════════════════╗
║     BCCR Scheduler - Pre-deployment Tests                 ║
╚════════════════════════════════════════════════════════════╝
    """)
    
    tests = [
        ("Docker Installation", test_docker_installation),
        ("Docker Compose", test_docker_compose),
        ("Python Packages", test_python_packages),
        (".env Configuration", test_env_file),
    ]
    
    results = {}
    for test_name, test_func in tests:
        try:
            results[test_name] = test_func()
        except Exception as e:
            print(f"❌ Unexpected error in {test_name}: {str(e)}")
            results[test_name] = False
    
    # Print summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    
    for test_name, passed in results.items():
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{status}: {test_name}")
    
    all_passed = all(results.values())
    
    print(f"\n{'='*60}")
    if all_passed:
        print("✓ All tests passed! Ready to deploy scheduler.")
        print(f"{'='*60}")
        print("""
Next steps:
1. cd MSSQL
2. docker-compose down (if running)
3. docker-compose up -d --build
4. docker logs -f bccr-scheduler
5. Check that scheduler starts at 5:00 AM daily
        """)
    else:
        print("✗ Some tests failed. Please fix the issues above.")
        print(f"{'='*60}")
        sys.exit(1)

if __name__ == "__main__":
    main()
