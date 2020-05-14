import pyspark

def apply_effective_dating(df):
    df.withcolumn("date")