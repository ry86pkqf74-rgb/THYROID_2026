#!/usr/bin/env python3
"""Convenience: pretty-print all registered tasks + their model assignments.

Run from anywhere with the package installed:
    python -m thyroid_mlx_extract.cli list-tasks
"""
import subprocess
import sys


def main():
    try:
        result = subprocess.run(
            ["thyroid-mlx", "list-tasks"],
            capture_output=True,
            text=True,
            check=True,
        )
        print(result.stdout)
    except FileNotFoundError:
        print("Error: `thyroid-mlx` CLI not found. Install with:")
        print("  cd tools/thyroid_mlx_extract && pip install -e .")
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        print(f"Error: {e.stderr}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
