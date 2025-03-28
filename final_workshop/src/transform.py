import pandas as pd

# Check for missing values and frequency of the most common values
def show_most_frequent_values(df, columns=None, top_n=5):
    """
    Displays the most frequent values in the specified columns of the DataFrame.
    
    Args:
    df (pd.DataFrame): DataFrame containing the columns.
    columns (list, optional): List of column names to check. If None, all columns are checked.
    top_n (int, optional): Number of top frequent values to display. Default is 5.
    
    Returns:
    None
    """
    if columns is None:
        columns = df.columns
    for column in columns:
        try:
            print(f"Column: {column}")
            # Convert column to string if it contains non-1-dimensional data
            if df[column].apply(lambda x: isinstance(x, (list, dict))).any():
                df[column] = df[column].apply(str)
            print(df[column].value_counts().head(top_n))
            print("\n")
        except ValueError as e:
            print(f"Could not process column {column}: {e}")
            print("\n")

# Transform Aspect Ratio Img Size Columns
def transform_imgsize_aspect_ratio(row, width_col, height_col):
    """
    Determines the aspect ratio of an image based on its width and height.
    
    Args:
    row (pd.Series): Row containing the width and height columns.
    width_col (str): Name of the column containing the width.
    height_col (str): Name of the column containing the height.
    
    Returns:
    str: Aspect ratio category ('SQUARE', 'LANDSCAPE', 'PORTRAIT').
    """
    if row[width_col] == row[height_col]:
        return 'SQUARE'
    elif row[width_col] > row[height_col]:
        return 'LANDSCAPE'
    else:
        return 'PORTRAIT'

def transform_img_quality(row, width_col, height_col):
    """
    Determines the quality of an image based on its resolution.
    
    Args:
    row (pd.Series): Row containing the width and height columns.
    width_col (str): Name of the column containing the width.
    height_col (str): Name of the column containing the height.
    
    Returns:
    str: Image quality category ('8K', '4K', 'ULTRAHD', 'FULLHD', 'HD', 'XGA', 'SD', 'QVGA', 'LOWERSCALES').
    """
    resolution = row[width_col] * row[height_col]
    if resolution >= 7680 * 4320:
        return '8K'
    elif resolution >= 4096 * 2160:
        return '4K'
    elif resolution >= 3840 * 2160:
        return 'ULTRAHD'
    elif resolution >= 1920 * 1080:
        return 'FULLHD'
    elif resolution >= 1280 * 720:
        return 'HD'
    elif resolution >= 1024 * 768:
        return 'XGA'
    elif resolution >= 640 * 480:
        return 'SD'
    elif resolution >= 320 * 240:
        return 'QVGA'
    else:
        return 'LOWERSCALES'

# Standardize / Transform Date Columns
def transform_date_by_range(df, date_col):
    """
    Transforms a date column into multiple date-related columns.
    
    Args:
    df (pd.DataFrame): DataFrame containing the date column.
    date_col (str): Name of the date column.
    
    Returns:
    pd.DataFrame: DataFrame with new date-related columns.
    """
    df[date_col] = pd.to_datetime(df[date_col])
    df[date_col+'_year'] = df[date_col].dt.year
    df[date_col+'_month'] = df[date_col].dt.month
    df[date_col+'_day'] = df[date_col].dt.day
    df[date_col+'_hour'] = df[date_col].dt.hour
    df[date_col+'_minute'] = df[date_col].dt.minute
    
    # Mapping month and day
    month_map = {1: 'January', 2: 'February', 3: 'March', 4: 'April', 5: 'May', 6: 'June', 7: 'July', 8: 'August', 9: 'September', 10: 'October', 11: 'November', 12: 'December'}
    day_map = {0: 'Monday', 1: 'Tuesday', 2: 'Wednesday', 3: 'Thursday', 4: 'Friday', 5: 'Saturday', 6: 'Sunday'}
    
    df[date_col+'_month'] = df[date_col].dt.month.map(month_map)
    df[date_col+'_day_off_week'] = df[date_col].dt.dayofweek.map(day_map)
    df[date_col+'_minute'] = df[date_col].dt.minute
    #df = df.drop(columns=[date_col])
    return df

def sum_columns(row, columns):
    return row[columns].sum()

def subtract_columns(df, col1, col2, new_col_name):
    """
    Subtracts the values of col2 from the values of col1 and adds the result as a new column.
    
    Args:
    df (pd.DataFrame): DataFrame containing the columns.
    col1 (str): Name of the first column.
    col2 (str): Name of the second column.
    new_col_name (str): Name of the new column that will contain the result of the subtraction.
    
    Returns:
    pd.DataFrame: DataFrame with the new column added.
    """
    df[new_col_name] = df[col1] - df[col2]
    return df

def groupby_tail_sorted(df, feature, date_col):
    """
    Sorts the DataFrame by the date column in ascending order, groups by the specified feature, 
    and returns the last row of each group.
    
    Args:
    df (pd.DataFrame): DataFrame to be sorted and grouped.
    feature (str): Name of the column to group by.
    date_col (str): Name of the date column to sort by.
    
    Returns:
    pd.DataFrame: DataFrame containing the last row of each group after sorting.
    """
    # Ensure the DataFrame is sorted by the date column in ascending order
    df = df.sort_values(by=date_col, ascending=True)
    
    # Group by the feature and get the last row of each group
    return df.groupby(feature).tail(1)

def fillna_columns(df, columns, value):
    """
    Fills NaN values in the specified columns with the given value.
    
    Args:
    df (pd.DataFrame): DataFrame containing the columns.
    columns (list): List of column names to fill NaN values.
    value: Value to replace NaN values with.
    
    Returns:
    pd.DataFrame: DataFrame with NaN values filled in the specified columns.
    """
    df[columns] = df[columns].fillna(value)
    return df

def compare_columns(row, col1, col2):
    """
    Compares two columns in a DataFrame row and returns True if they are equal, else False.
    
    Args:
    row (pd.Series): Row containing the columns to compare.
    col1 (str): Name of the first column.
    col2 (str): Name of the second column.
    
    Returns:
    bool: True if the columns are equal, else False.
    """
    return row[col1] == row[col2]