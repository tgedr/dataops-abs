
"""ETL4GH module for GitHub Actions ETL workflows.

Provides abstract base class for Extract, Transform, Load operations
specifically designed for GitHub Actions environments.
"""
from abc import ABC
import logging
import os
from typing import Any

from tgedr_dataops_abs.etl import Etl


logger = logging.getLogger(__name__)


class Etl4GH(Etl, ABC):
    """Abstract base class for ETL (Extract, Transform, Load) operations.

    Provides a template method pattern for ETL workflows with configuration
    injection and optional validation hooks.
    This class is specifically designed for GitHub Actions workflows, where the
    result of the ETL process is written to the GitHub Actions output file specified by the GITHUB_OUTPUT environment variable.
    """

    def run(self) -> None:
        """Execute the complete ETL workflow.

        Runs extract, validate_extract, transform, validate_transform, and load
        in sequence with structured logging.
        In the end writes the result to the GitHub Actions output file specified by the
        GITHUB_OUTPUT environment variable.

        Returns
        -------
        None
        """
        logger.info("[run|in]")

        self.extract()
        self.validate_extract()

        self.transform()
        self.validate_transform()

        result: Any = self.load()

        with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as f:  # noqa: PTH123
            f.write(f"result={result}\n")

        logger.info("[run|out] => %s", result)

