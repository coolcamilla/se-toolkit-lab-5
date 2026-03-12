"""Test script for fetch_logs function."""

import asyncio
import sys
from pathlib import Path

# Add backend to path
backend_dir = Path(__file__).resolve().parent / "backend"
sys.path.insert(0, str(backend_dir))

from app.etl import fetch_logs


async def main():
    print("Testing fetch_logs()...")
    try:
        logs = await fetch_logs()
        print(f"[OK] Successfully fetched {len(logs)} logs")
        if logs:
            print(f"First log: {logs[0]}")
    except Exception as e:
        print(f"[ERROR] {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
