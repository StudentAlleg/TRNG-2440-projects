#TODO main running of program
import argparse
import logging
import os

from pandas.core.interchange.dataframe_protocol import DataFrame

from src.clean import clean
from src.database import Database
from src.extract import extract
from src.load import load

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

if __name__ == "__main__":
    #Extract
    parser = argparse.ArgumentParser(
        prog="Open Meteo API", description="Gets data from Open Meteo API"
    )

    data_dir = os.path.join(os.path.dirname(__file__), "", "data")
    parser.add_argument('-s', '--start', type=str, dest="start_date", default="2026-05-01")
    parser.add_argument('-e', '--end', type=str, dest="end_date", default="2026-06-01")
    parser.add_argument('-l', '--location', type=str, dest="location_file", default=os.path.join(data_dir, "locations.json"))
    parser.add_argument('-o', '--out', type=str, dest="out_file", default=os.path.join(data_dir, "api_out.json"))
    parser.add_argument('--no-extract', action='store_true', dest="no_extract", help="Skip the extract step")


    args = parser.parse_args()
    database: Database = Database()
    # create the tables
    with database.get_connection() as conn, open(os.path.join(os.path.dirname(__file__), "sql", "create_database.sql"), "r") as ct:
        conn.execute(ct.read())
    if not args.no_extract:
        extract(start_date=args.start_date, end_date=args.end_date, locations_file=args.location_file, out_file=args.out_file)
        logging.info("Data extracted")
    dataframe: DataFrame = clean(api_out_path=args.out_file, locations_path=args.location_file)
    logging.info("Data cleaned")
    load(dataframe=dataframe, database=database)
    logging.info("Data loaded successfully")






