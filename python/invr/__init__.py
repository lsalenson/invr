"""
invr — declarative data validation for pandas DataFrames.

Quick start::

    import pandas as pd
    import invr

    df = pd.read_csv("data.csv")
    report = invr.run("spec.yaml", df)

    if report.failed():
        for v in report.violations:
            print(v)
"""

from .invr_core import Report, Violation, run, run_file

__all__ = ["run", "run_file", "Report", "Violation"]
