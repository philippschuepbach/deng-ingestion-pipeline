from __future__ import annotations

import pandas as pd


def replace_lookup_table(
    *,
    df: pd.DataFrame,
    engine,
    schema: str,   
    table_name: str,
) -> None:
    """Replace a lookup table fully with the given dataframe."""
    df.to_sql(
        name=table_name,
        con=engine,
        schema=schema,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=1000,
    )