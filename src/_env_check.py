import sys, subprocess
print("Python:", sys.executable)
print("Version:", sys.version)
for m in ["duckdb","pandas","pyarrow","openpyxl","matplotlib","yaml"]:
    try:
        __import__(m)
        print(f"OK: {m}")
    except Exception as e:
        print(f"FAIL: {m} -> {e}")
