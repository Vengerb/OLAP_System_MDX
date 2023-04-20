from ntpath import join
from os import listdir
from os.path import join, isfile
from olapy.core.mdx.executor import MdxEngine
import pandas as pd
import numpy as np

def ShowRowsTable(path, sep = ","):
    import pandas as pd
    return list(pd.read_csv(path, sep=sep).columns)

def ShowRowsTables(array_table, path_folder_cube, sep = ","):
    result = {}
    for table in array_table:
        result[table.replace(".csv","")] = ShowRowsTable(join(path_folder_cube, table), sep)
    return result


def main():
    EXECUTER = MdxEngine() # instantiate the MdxEngine
    PATH_TO_OLAP_DATA = join(EXECUTER.olapy_data_location,EXECUTER.cubes_folder)
    NAME_CUBE = "sales"
    path_folder_cube = join(PATH_TO_OLAP_DATA, NAME_CUBE)
    array_table = listdir(path_folder_cube)
    EXECUTER.load_cube(NAME_CUBE) # load sales cube
    
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
		[Geography].[Continent].[Europe]} ON COLUMNS
        {
		[Measures].[Count],
		[Measures].[Amount]} ON ROWS
        FROM [Sales]

        WHERE [Time].[Year].[2011]
        or
        WHERE [Time].[Year].[2010]
    """

    print("PATH_TO_OLAP_DATA: ", PATH_TO_OLAP_DATA, "\n"*2)

    print(ShowRowsTables(array_table, path_folder_cube, sep = ";"), "\n"*2)

    df = EXECUTER.execute_mdx(query6)["result"]
    print("result query: \n", df)

main()