import sys
from football_driver import FootballSparkDriver
from pyspark.sql import DataFrame

if __name__ == '__main__':
    input_path: str = sys.argv[1]
    output_path: str = sys.argv[2]

    driver: FootballSparkDriver = FootballSparkDriver("playersStats")

    # Start spark session
    driver.start()

    # get a dataframe from CSV
    df: DataFrame = driver.get_data(input_path, protocol="file")

    print("Reading completed. Cleaning process started ...")

    # clean each dataframe column
    for col_name in df.columns:
        df = driver.clean_column(df, col_name)

    print("Cleaning completed. Writing process started ...")

    # df.printSchema()

    # df.show(50)

    # write resulting dataframe to file
    driver.save(df, output_path)

    print("Writing completed.")

    # Stop spark session
    driver.stop()
