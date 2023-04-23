from olapy.core.mdx.executor import MdxEngine
import pandas as pd
import numpy as np

def main():
    EXECUTER = MdxEngine() # instantiate the MdxEngine
    NAME_CUBE = "sales"
    EXECUTER.load_cube(NAME_CUBE, "Facts", ";", ['Amount', 'Count']) # load sales cube
    
    query1 = """
        SELECT
        {[City]}
        ON COLUMNS,
        {[Year]}
        ON ROWS
        FROM [Black_Friday]
        Where
        [Average Spent By Person]
    """

    query2 = """
        SELECT
        Hierarchize({[Measures].[Amount]}) ON COLUMNS
        FROM [sales]
    """

    query3 = """
        SELECT
        {[Amount]}
        ON COLUMNS
        FROM [sales]
        where
        [Time].[Day].[January 1,2010]
    """

    query4 = """
    SELECT
    {
    [Measures].[Amount]
    } ON COLUMNS
    {[Measures].[Time].[Time].[Month]} ON ROWS
    FROM [sales]
    WHERE [Time].[Time].[Year].[2010]
    """

    query5 = """
      SELECT
		{
		[Geography].[Continent].[America],
		[Geography].[Continent].[Europe]} ON COLUMNS

        FROM [Sales]

        WHERE [Time].[Year].[2011]
    """

    query6 = """
    SELECT
    {
        [Geography].[Continent].[America],
        [Geography].[Continent].[Europe]
    } 
    ON COLUMNS
    {
        [Measures].[Count],
        [Measures].[Amount]
    } 
    ON ROWS
    FROM ["""+NAME_CUBE+"""]
    WHERE 
        [Time].[Year].[2011]
    or
    WHERE 
        [Time].[Year].[2010]
    """

    query7 = """
      
    """

    QUERY = query6

    print("olapy_data_location: ", EXECUTER.olapy_data_location, "\n"*2)

    print("csv_files_cubes: ", EXECUTER.csv_files_cubes, "\n"*2)

    print("cube: ", EXECUTER.cube, "\n"*2)

    print("cube_config: ", EXECUTER.cube_config, "\n"*2)

    print("source_type: ", EXECUTER.source_type, "\n"*2)

    print("all_tables_names: ", EXECUTER.get_all_tables_names(), "\n"*2)

    print("table facts: ", EXECUTER.facts, "\n"*2)

    print("measures: ", EXECUTER.measures, "\n"*2)

    print("selected_measures: ", EXECUTER.selected_measures, "\n"*2)

    print("table and their comuns:\n")
    for table_name, table in EXECUTER.tables_loaded.items():
        print("{0}: {1}".format(table_name, list(table.columns)))
    print("\n")

    print("star_schema_dataframe: \n", EXECUTER.star_schema_dataframe.T, "\n"*2)

    print("query: \n", QUERY, "\n"*2)

    df = EXECUTER.execute_mdx(QUERY)["result"]

    print("result query: \n", df)

def main2():
    EXECUTER = MdxEngine() # instantiate the MdxEngine
    NAME_CUBE = "sales"
    #EXECUTER.load_cube(NAME_CUBE, "Facts", ";", ['Amount', 'Count']) # load sales cube    

main()