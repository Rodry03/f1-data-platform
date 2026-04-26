import duckdb

con = duckdb.connect("f1_data.duckdb")
con.execute("COPY raw_races TO 'f1_dbt/seeds/raw_races.csv' (HEADER, DELIMITER ',')")
con.execute("COPY raw_results TO 'f1_dbt/seeds/raw_results.csv' (HEADER, DELIMITER ',')")
con.execute("COPY raw_fastest_laps TO 'f1_dbt/seeds/raw_fastest_laps.csv' (HEADER, DELIMITER ',')")
print("CSVs exportados")
con.close()