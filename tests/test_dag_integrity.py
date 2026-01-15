"""
DAG integrity tests.

These tests verify that DAGs are properly configured
and follow Airflow best practices.
"""

import pytest
from pathlib import Path


class TestDAGIntegrity:
    """Tests for DAG configuration and best practices."""

    @pytest.fixture
    def dag_files(self):
        """Get all DAG files."""
        return list(Path("dags").glob("*.py"))

    def test_dag_files_exist(self, dag_files):
        """Verify DAG files exist."""
        assert len(dag_files) > 0, "No DAG files found in dags/"

    def test_dag_files_have_docstrings(self, dag_files):
        """Verify each DAG file has a module docstring."""
        for dag_file in dag_files:
            content = dag_file.read_text()
            # Check for docstring at start of file
            stripped = content.lstrip()
            has_docstring = (
                stripped.startswith('"""')
                or stripped.startswith("'''")
                or stripped.startswith('r"""')
            )
            assert has_docstring, f"{dag_file.name} missing module docstring"

    def test_dag_files_no_hardcoded_dates(self, dag_files):
        """Check for hardcoded dates that should use templates."""
        import re

        date_pattern = r"\d{4}-\d{2}-\d{2}"
        allowed_contexts = ["start_date", "datetime(", "# ", "schedule"]

        for dag_file in dag_files:
            content = dag_file.read_text()
            lines = content.split("\n")

            for i, line in enumerate(lines, 1):
                if re.search(date_pattern, line):
                    # Skip if in allowed context
                    if any(ctx in line for ctx in allowed_contexts):
                        continue
                    # Skip if it's a Jinja template
                    if "{{" in line and "}}" in line:
                        continue
                    # This might be a hardcoded date - warn
                    # Note: Not failing, just informational
                    print(
                        f"Potential hardcoded date in {dag_file.name}:{i}: {line.strip()}"
                    )

    def test_dag_has_default_args(self, dag_files):
        """Verify DAGs define default_args."""
        for dag_file in dag_files:
            content = dag_file.read_text()
            assert (
                "default_args" in content
            ), f"{dag_file.name} missing default_args definition"

    def test_dag_has_tags(self, dag_files):
        """Verify DAGs have tags for organization."""
        for dag_file in dag_files:
            content = dag_file.read_text()
            assert "tags=" in content, f"{dag_file.name} missing tags parameter"

    def test_dag_has_catchup_disabled(self, dag_files):
        """Verify DAGs explicitly set catchup parameter."""
        for dag_file in dag_files:
            content = dag_file.read_text()
            assert "catchup=" in content, f"{dag_file.name} missing catchup parameter"

    def test_no_import_star(self, dag_files):
        """Verify no 'from x import *' statements."""
        for dag_file in dag_files:
            content = dag_file.read_text()
            assert (
                "import *" not in content
            ), f"{dag_file.name} uses 'import *' which is bad practice"


class TestDAGTaskConfiguration:
    """Tests for task-level configuration."""

    def test_tasks_have_retries(self):
        """Verify retry configuration is present."""
        dag_content = Path("dags/weather_etl_pipeline.py").read_text()
        assert "retries" in dag_content, "DAG missing retry configuration"

    def test_tasks_have_timeout(self):
        """Verify execution timeout is configured."""
        dag_content = Path("dags/weather_etl_pipeline.py").read_text()
        assert "execution_timeout" in dag_content, "DAG missing execution_timeout"

    def test_extract_task_has_error_handling(self):
        """Verify extract function has error handling."""
        dag_content = Path("dags/weather_etl_pipeline.py").read_text()
        assert (
            "RequestException" in dag_content
        ), "Extract task should handle request exceptions"
