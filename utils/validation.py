def validate_amounts(df):
    bad_rows = df.filter(df.amount <= 0).count()
    return bad_rows == 0

def validate_dates(df):
    return df.filter(df.date.isNull()).count() == 0
