"""
invars — declarative data validation for pandas DataFrames.

Quick start::

    import pandas as pd
    import invars

    df = pd.read_csv("data.csv")
    report = invars.run("spec.yaml", df)

    if report.failed():
        for v in report.violations:
            print(v)
"""

from .invars_core import Report, Violation, run, run_file

__all__ = ["run", "run_file", "Report", "Violation"]
