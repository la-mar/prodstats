try:
    import coverage

    coverage.process_startup()
except ImportError:
    print("Failed to import coverage.py")
