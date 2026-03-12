"""Test script for fetch_items function."""

import asyncio
import sys
from pathlib import Path

# Add backend to path
backend_dir = Path(__file__).resolve().parent / "backend"
sys.path.insert(0, str(backend_dir))

from app.etl import fetch_items


async def main():
    print("Testing fetch_items()...")
    try:
        items = await fetch_items()
        print(f"[OK] Successfully fetched {len(items)} items")
        if items:
            print(f"First item: {items[0]}")
    except Exception as e:
        print(f"[ERROR] {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
