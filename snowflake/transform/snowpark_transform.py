import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col
import pandas as pd

def main(session: snowpark.Session):
    tableName = 'Food_Market_DB.RAW.RAW_DATA'
    snow_table = session.table(tableName)

    df = snow_table.to_pandas()
    print(df.shape)

    tableName = 'Food_Market_DB.RAW.ROW_COUNT'
    row_count_table = session.table(tableName)
    row_count_df = row_count_table.to_pandas()

    if row_count_df.shape[0] != 0:
        row_count_df = row_count_df[row_count_df['INDEX_NO'] == max(row_count_df['INDEX_NO'])].reset_index(drop=True)
        old_rows = row_count_df['NO_OF_ROWS'][0]

        
        df = df.sort_values(by='YEAR').reset_index(drop=True)
        df1 = df.tail(df.shape[0] - old_rows)
    else:
        df1 = df

    print('Shape 1:', df1.shape)
    print('Shape 1:', df1['DATES'].unique())

    df_notnull = df1.loc[:, df1.notnull().any()]

    keep_cols = ['COUNTRY', 'MKT_NAME', 'DATES']
    df_1 = df_notnull[keep_cols + [c for c in df_notnull.columns if c.startswith(('O_', 'H_', 'L_', 'C_', 'INFLATION_', 'TRUST_'))]]

    final_component_df = df_1.melt(id_vars=keep_cols, var_name='ITEM', value_name='VALUE')

    final_component_df.insert(loc=4, column='TYPE', value=final_component_df['ITEM'].str.split('_').str[0])
    final_component_df['TYPE'] = final_component_df['TYPE'].replace({'O': 'OPEN', 'H': 'HIGH', 'L': 'LOW', 'C': 'CLOSE'})
    final_component_df['ITEM'] = final_component_df['ITEM'].str.split('_').str[1]

    pivot_type_df = final_component_df.pivot_table(
        index=['COUNTRY', 'MKT_NAME', 'DATES', 'ITEM'],
        values='VALUE',
        columns='TYPE'
    ).reset_index()

    present_cols = pivot_type_df.columns.tolist()
    expected_cols = ['COUNTRY', 'MKT_NAME', 'DATES', 'ITEM', 'CLOSE', 'HIGH', 'INFLATION', 'LOW', 'OPEN', 'TRUST']

    add_cols = list(set(expected_cols) - set(present_cols))
    if add_cols:
        for c in add_cols:
            pivot_type_df[c] = 0

    final_df = pivot_type_df[expected_cols]

    print('Shape CLEANSED_FOOD_DATA_NEW:', pivot_type_df.shape)
    print(pivot_type_df['DATES'].unique())

    if row_count_df.shape[0] == 0:
        d = {
            'INDEX_NO': 1,
            'NO_OF_ROWS': df.shape[0],
            'DATE': pd.Timestamp.now(),
            'NO_OF_ROWS_ADDED': df.shape[0],
            'NO_OF_ROWS_IN_TRANSFORMED_TABLE': pivot_type_df.shape[0]
        }
    else:
        d = {
            'INDEX_NO': int(row_count_df['INDEX_NO'][0]) + 1,
            'NO_OF_ROWS': df.shape[0],
            'DATE': pd.Timestamp.now(),
            'NO_OF_ROWS_ADDED': int(df.shape[0] - old_rows),
            'NO_OF_ROWS_IN_TRANSFORMED_TABLE': pivot_type_df.shape[0]
        }


    session.create_dataframe(pd.DataFrame([d])).write.save_as_table(
        'Food_Market_DB.RAW.ROW_COUNT',
        mode="append"
    )

    snow_component = session.create_dataframe(final_df)
    snow_component.write.save_as_table(
        "Food_Market_DB.CLEAN.CLEANSED_FOOD_DATA_NEW",
        mode="append"
    )

    return snow_component