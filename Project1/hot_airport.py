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

    hot_airport = sqlc.sql('''
        SELECT iata, city, year, NUM_dpt FROM
            (SELECT iata, city, year, NUM_dpt, ROW_NUMBER() OVER (PARTITION BY year ORDER BY year, NUM_dpt DESC) AS Annual_Rank FROM
                (SELECT iata, city, year, COUNT(*) AS NUM_dpt FROM 
                    (SELECT airport_codes.iata, airport_codes.city, dpt.Year FROM
                    dpt LEFT JOIN airport_codes ON dpt.usg_apt = airport_codes.iata
                    WHERE Year IS NOT NULL AND iata IS NOT NULL)
                GROUP BY iata, city, year
                ORDER BY year, NUM_dpt DESC))
        WHERE Annual_Rank<=10        
    ''')

    hot_airport.coalesce(1).write.csv("project_hot_airport", sep = ",")