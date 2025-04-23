def filter_active_users(df):
    return df.filter(df.status=="active")