from pyspark import SparkContext
from pyspark.sql import SQLContext

if __name__ == '__main__':
    sc = SparkContext(appName = "umsi618f22project")
    sqlc = SQLContext(sc)

    dpt = sqlc.read.csv('archive/International_Report_Departures.csv', header = True)
    airport_codes = sqlc.read.csv('archive/airports.csv', header = True)
    country_codes = sqlc.read.csv('archive/country-and-continent-codes-list-csv.csv', header = True)

    sqlc.registerDataFrameAsTable(dpt, 'dpt')
    sqlc.registerDataFrameAsTable(airport_codes, 'airport_codes')
    sqlc.registerDataFrameAsTable(country_codes, 'country_codes')

    hot_destination = sqlc.sql('''
        SELECT Year, Country_Name, NUM_dpt FROM
            (SELECT Year, Country_Name, NUM_dpt, ROW_NUMBER() OVER (PARTITION BY Year ORDER BY NUM_dpt DESC) AS Annual_Rank FROM 
                (SELECT dpt.Year, T2.Country_Name, COUNT(*) AS NUM_dpt FROM dpt LEFT JOIN
                    (SELECT airport_codes.iata, country_codes.Continent_Name, country_codes.Country_Name FROM airport_codes 
                    LEFT JOIN country_codes ON airport_codes.country = country_codes.Two_Letter_Country_Code
                    WHERE iata IS NOT NULL) AS T2
                ON dpt.fg_apt = T2.iata
                WHERE Country_Name IS NOT NULL
                GROUP BY Year, Country_Name
                ORDER BY Year, NUM_dpt DESC))
        WHERE Annual_Rank <= 10
    ''')

    hot_destination.coalesce(1).write.csv("project_hot_destination", sep = ",")