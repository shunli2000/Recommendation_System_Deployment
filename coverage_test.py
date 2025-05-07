import pytest

if __name__ == "__main__":
    pytest.main([
        "--cov=.",
        "--cov-report=term-missing",
        "--cov-report=xml",
        "--cov-report=html",
        "--junitxml=report.xml",
        "tests/"
    ])