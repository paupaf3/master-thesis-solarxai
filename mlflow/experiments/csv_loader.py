import pandas as pd
# import io

def load_csv(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    if not lines:
        return pd.DataFrame()

    # 1. Get header and count target columns
    header = lines[0].strip().split(',')
    num_cols = len(header)
    
    # 2. Find where the JSON is (it's always the column before the last 4)
    # The last 4 are always: ingestion_timestamp, source, processing_status, error_message
    json_idx = header.index('raw_json')
    cols_after_json = num_cols - (json_idx + 1)

    data_rows = []
    for line in lines[1:]:
        line = line.strip()
        if not line:
            continue
        
        parts = line.split(',')
        
        # Leading columns (before JSON)
        leading = parts[:json_idx]
        
        # Trailing columns - take exactly the last cols_after_json columns
        trailing = parts[-cols_after_json:]

        # Everything in between is the JSON
        json_parts = parts[json_idx:-cols_after_json]
        json_blob = ",".join(json_parts).strip('"')

        # Combine and ensure we only have the exact number of columns as the header
        full_row = leading + [json_blob] + trailing
        data_rows.append(full_row[:num_cols])

    # Drop columns that are entirely empty or whitespace-only like 'error_message'
    df = pd.DataFrame(data_rows, columns=header)
    df = df.replace(r"^\s*$", pd.NA, regex=True)
    df = df.dropna(axis=1, how='all')

    return df