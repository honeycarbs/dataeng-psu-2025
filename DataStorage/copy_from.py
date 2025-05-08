# this program copy_from_csvs Census ACS data using basic, slow INSERTs 
# run it with -h to see the command line options

import time
import psycopg2
import argparse
import re
import csv

DBname = "postgres"
DBuser = "postgres"
DBpwd = "postgres"   # insert your postgres db password here
TableName = 'CensusData'
Datafile = 'acs2017_census_tract_data.csv'  # name of the data file to be copy_from_csved
CreateDB = True  # indicates whether the DB table should be (re)-created

def row2vals(row):
	for key in row:
		if not row[key]:
			row[key] = 0  # ENHANCE: handle the null vals
		row['County'] = row['County'].replace('\'','')  # TIDY: eliminate quotes within literals

	ret = f"""
	   {row['TractId']},			-- TractId
	   '{row['State']}',				-- State
	   '{row['County']}',			   -- County
	   {row['TotalPop']},			   -- TotalPop
	   {row['Men']},					-- Men
	   {row['Women']},				  -- Women
	   {row['Hispanic']},			   -- Hispanic
	   {row['White']},				  -- White
	   {row['Black']},				  -- Black
	   {row['Native']},				 -- Native
	   {row['Asian']},				  -- Asian
	   {row['Pacific']},				-- Pacific
	   {row['VotingAgeCitizen']},	   -- VotingAgeCitizen
	   {row['Income']},				 -- Income
	   {row['IncomeErr']},			  -- IncomeErr
	   {row['IncomePerCap']},		   -- IncomePerCap
	   {row['IncomePerCapErr']},		-- IncomePerCapErr
	   {row['Poverty']},				-- Poverty
	   {row['ChildPoverty']},		   -- ChildPoverty
	   {row['Professional']},		   -- Professional
	   {row['Service']},				-- Service
	   {row['Office']},				 -- Office
	   {row['Construction']},		   -- Construction
	   {row['Production']},			 -- Production
	   {row['Drive']},				  -- Drive
	   {row['Carpool']},				-- Carpool
	   {row['Transit']},				-- Transit
	   {row['Walk']},				   -- Walk
	   {row['OtherTransp']},			-- OtherTransp
	   {row['WorkAtHome']},			 -- WorkAtHome
	   {row['MeanCommute']},			-- MeanCommute
	   {row['Employed']},			   -- Employed
	   {row['PrivateWork']},			-- PrivateWork
	   {row['PublicWork']},			 -- PublicWork
	   {row['SelfEmployed']},		   -- SelfEmployed
	   {row['FamilyWork']},			 -- FamilyWork
	   {row['Unemployment']}			-- Unemployment
	"""

	return ret


def initialize():
  parser = argparse.ArgumentParser()
  parser.add_argument("-d", "--datafile", required=True)
  parser.add_argument("-c", "--createtable", action="store_true")
  args = parser.parse_args()

  global Datafile
  Datafile = args.datafile
  global CreateDB
  CreateDB = args.createtable

# connect to the database
def dbconnect():
	connection = psycopg2.connect(
		host="localhost",
		database=DBname,
		user=DBuser,
		password=DBpwd,
	)
	connection.autocommit = False
	return connection

# create the target table 
# assumes that conn is a valid, open connection to a Postgres database
def createTable(conn):

	with conn.cursor() as cursor:
		cursor.execute(f"""
			DROP TABLE IF EXISTS {TableName};
			CREATE TABLE {TableName} (
				TractId			 NUMERIC,
				State			   TEXT,
				County			  TEXT,
				TotalPop			INTEGER,
				Men				 INTEGER,
				Women			   INTEGER,
				Hispanic			DECIMAL,
				White			   DECIMAL,
				Black			   DECIMAL,
				Native			  DECIMAL,
				Asian			   DECIMAL,
				Pacific			 DECIMAL,
				VotingAgeCitizen	DECIMAL,
				Income			  DECIMAL,
				IncomeErr		   DECIMAL,
				IncomePerCap		DECIMAL,
				IncomePerCapErr	 DECIMAL,
				Poverty			 DECIMAL,
				ChildPoverty		DECIMAL,
				Professional		DECIMAL,
				Service			 DECIMAL,
				Office			  DECIMAL,
				Construction		DECIMAL,
				Production		  DECIMAL,
				Drive			   DECIMAL,
				Carpool			 DECIMAL,
				Transit			 DECIMAL,
				Walk				DECIMAL,
				OtherTransp		 DECIMAL,
				WorkAtHome		  DECIMAL,
				MeanCommute		 DECIMAL,
				Employed			INTEGER,
				PrivateWork		 DECIMAL,
				PublicWork		  DECIMAL,
				SelfEmployed		DECIMAL,
				FamilyWork		  DECIMAL,
				Unemployment		DECIMAL
			);
		""")

		print(f"Created {TableName}")
		
def add_constraints_and_indexes(conn):
	"""Add primary key and indexes after data copy_from_csving"""
	with conn.cursor() as cursor:
		cursor.execute(f"""
			ALTER TABLE {TableName} ADD PRIMARY KEY (TractId);
			CREATE INDEX idx_{TableName}_State ON {TableName}(State);
		""")
		print(f"Added constraints and indexes to {TableName}")

def copy_from_csv(conn, csv_path):
	cur = conn.cursor()
	start = time.perf_counter()
	
	with open(csv_path, 'r') as f:
		next(f)
		cur.copy_from(f, TableName.lower(), sep=',', null='')
	conn.commit()
	
	elapsed = time.perf_counter() - start
	print(f'Finished Loading. Elapsed Time: {elapsed:0.4} seconds')




def main():
	initialize()
	conn = dbconnect()

	if CreateDB:
		createTable(conn)

	copy_from_csv(conn, Datafile)
	add_constraints_and_indexes(conn)


if __name__ == "__main__":
	main()