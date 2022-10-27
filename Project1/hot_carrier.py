from pyspark import SparkContext
from pyspark.sql import SQLContext

if __name__ == '__main__':
    sc = SparkContext(appName = "umsi618f22project")
    sqlc = SQLContext(sc)

    dpt = sqlc.read.csv('archive/International_Report_Departures.csv', header = True)
    carrier_codes = sqlc.read.csv('archive/airline_codes.csv', header = True)
    US_carrier_codes = sqlc.read.csv('archive/US_airline_codes.csv', header = True)

    sqlc.registerDataFrameAsTable(dpt, 'dpt')
    sqlc.registerDataFrameAsTable(carrier_codes, 'carrier_codes')
    sqlc.registerDataFrameAsTable(US_carrier_codes, 'US_carrier_codes')

    hot_carrier = sqlc.sql('''
        SELECT Year, Airline, carrier, NUM_dpt FROM 
            (SELECT Year, Airline, carrier,NUM_dpt, ROW_NUMBER() OVER (PARTITION BY Year ORDER BY Year, NUM_dpt DESC) AS Annual_Rank FROM
                (SELECT Year, Airline, carrier, COUNT(*) as NUM_dpt FROM 
                    (SELECT dpt.Year, dpt.carrier ,carrier_codes.Description AS Airline FROM 
                    dpt LEFT JOIN carrier_codes ON dpt.carrier = carrier_codes.Code)
                GROUP BY Year, Airline, carrier))
        WHERE Annual_Rank <= 10
    ''')

    hot_carrier.coalesce(1).write.csv("project_hot_carrier", sep = ",")

    international_hot_carrier = sqlc.sql('''
        SELECT Year, Airline, carrier, NUM_dpt FROM 
            (SELECT Year, Airline, carrier,NUM_dpt, ROW_NUMBER() OVER (PARTITION BY Year ORDER BY Year, NUM_dpt DESC) AS Annual_Rank FROM
                (SELECT Year, Airline, carrier,COUNT(*) as NUM_dpt FROM 
                    (SELECT dpt.Year, dpt.carrier ,T1.Description AS Airline FROM 
                    dpt LEFT JOIN 
                        (SELECT carrier_codes.Code, carrier_codes.Description, US_carrier_codes.Airline FROM carrier_codes LEFT JOIN US_carrier_codes ON
                        carrier_codes.Code = US_carrier_codes.IATA
                        WHERE Airline IS NULL) AS T1
                    ON dpt.carrier = T1.Code)
                WHERE Airline IS NOT NULL
                GROUP BY Year, Airline, carrier))
        WHERE Annual_Rank <= 15
    ''')

    international_hot_carrier.coalesce(1).write.csv("project_international_hot_carrier", sep = ',')
