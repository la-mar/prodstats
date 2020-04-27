try:
    import coverage

    coverage.process_startup()
except ImportError:
    print(f"Failed to import coverage.py")
