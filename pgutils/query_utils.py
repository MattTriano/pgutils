import os
import pandas as pd

def make_create_table_query(details_df, table_schema, pk_col=None):
    """Run this query
        # SELECT * from information_schema.columns
        # WHERE table_name = '<table_name>';
    then copy the HTML table to a string info_schema_str, and make a df via
        details_df = pd.read_html(info_schema_str)[0]
    """
    lines = []
    table_name = None
    for _, row in details_df.iterrows():
        if not table_name and row["table_name"]:
            table_name = row["table_name"]
        if pk_col:
            if row["column_name"].lower() == pk_col.lower():
                continue
        line = ["   "]
        line.append(row["column_name"])
        if row["data_type"] == row["data_type"]:
            if row["data_type"].lower() == "text":
                line.append(row["data_type"].upper())
            elif row["data_type"].lower() == "character":
                if row["character_maximum_length"]:
                    line.append(f"CHAR({int(row['character_maximum_length'])})")
                else:
                    line.append("CHAR")
            elif row["data_type"].lower() in ["integer", "smallint", "bigint"]:
                if row["numeric_precision"] == 16:
                    line.append("SMALLINT")
                elif row["numeric_precision"] == 64:
                    line.append("BIGINT")
                else:
                    line.append("INTEGER")
            elif row["data_type"].lower() == "date":
                line.append("DATE")
            else:
                print(f"data type is {row['data_type']}")
        if row["column_default"] == row["column_default"]:
            line.append(f"DEFAULT {row['column_default']}")
        if row["is_nullable"].lower() == "no":
            line.append("NOT NULL")
        lines.append(line)
    if table_schema:
        table_name = f"{table_schema}.{table_name}"

    if pk_col:
        pk_line = f"    {pk_col} SERIAL PRIMARY KEY,\n"
    else:
        pk_line = "    id SERIAL PRIMARY KEY,\n"

    create_statement = f"CREATE TABLE {table_name} (\n"
    columns_and_types = ",\n".join([" ".join(line) for line in lines])
    closing = "\n);"

    create_query = create_statement + pk_line + columns_and_types + closing
    print(create_query)
    return create_query