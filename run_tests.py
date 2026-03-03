#!/usr/bin/env python3
"""
Test Runner for Phase 2

Runs all unit tests and generates a summary report.
"""

import sys
import os
import subprocess
from pathlib import Path

# Add project root to path
project_root = Path(__file__).resolve().parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "web" / "backend"))


def run_test_file(test_file):
    """Run a single test file and return results."""
    print(f"\n{'=' * 70}")
    print(f"Running: {test_file}")
    print("=" * 70)

    try:
        result = subprocess.run(
            [sys.executable, test_file], capture_output=True, text=True, timeout=60
        )

        print(result.stdout)
        if result.stderr:
            print("STDERR:", result.stderr)

        return result.returncode == 0
    except subprocess.TimeoutExpired:
        print(f"TIMEOUT: {test_file} took too long")
        return False
    except Exception as e:
        print(f"ERROR running {test_file}: {e}")
        return False


def main():
    """Run all tests."""
    print("=" * 70)
    print("PHASE 2: TESTING")
    print("=" * 70)

    test_files = [
        "tests/unit/test_order_tracker.py",
    ]

    results = {}

    for test_file in test_files:
        full_path = project_root / test_file
        if full_path.exists():
            success = run_test_file(str(full_path))
            results[test_file] = success
        else:
            print(f"WARNING: {test_file} not found")
            results[test_file] = False

    # Summary
    print("\n" + "=" * 70)
    print("TEST SUMMARY")
    print("=" * 70)

    passed = sum(1 for v in results.values() if v)
    total = len(results)

    for test_file, success in results.items():
        status = "PASS" if success else "FAIL"
        print(f"{status}: {test_file}")

    print(f"\nTotal: {passed}/{total} test files passed")

    if passed == total:
        print("\nALL TESTS PASSED!")
        return 0
    else:
        print(f"\n{total - passed} TEST FILE(S) FAILED")
        return 1


if __name__ == "__main__":
    sys.exit(main())
