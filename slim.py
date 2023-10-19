import re
import io
import os
import datetime
import argparse
import agate
from time import sleep
from zipfile import ZipFile

def main(reprocess=True):

	# Get a list of all files in the /data/raw/ directory
	zip_files = os.listdir(raw_path)

	# Filter directory to only the ZIP files we care about.
	zip_files = [f for f in zip_files if f.startswith('HPS_Week') and f.endswith('zip')]

	# Columns that should exist in every phase.
	cols_common = [
		'WEEK',
		'SCRAM',
		'EST_ST',
		'INCOME',
		'EEDUC',
		'RRACE',
		'RHISPANIC',
		'TBIRTH_YEAR',
		'THHLD_NUMKID',
	]

	# Columns that exist only in the earliest phases.
	cols_1 = [
		'ENROLL1',
		'ENROLL2',
		'ENROLL3',
	]

	# Columns that exist in later phases.
	cols_2 = [
		'ENRPUBCHK',
		'ENRPRVCHK',
		'ENRHMSCHK',
		'TENROLLPUB',
		'TENROLLPRV',
		'TENROLLHMSCH',
		'ENROLLNONE',
	]

	# Iterate over each ZIP file
	for zip_filename in zip_files:

		# Determine the survey year from the filenames.
		week_num = zip_filename.split('HPS_Week')[1][0:2]
		print(f'* Week {week_num}')
		year = '2020'
		if int(week_num) >= 22 and int(week_num) <= 40:
			year = '2021'
		elif int(week_num) >= 41 and int(week_num) <= 52:
			year = '2022'
		elif int(week_num) >= 53:
			year = '2023'

		# Put together paths and filenames
		zip_filepath = os.path.join(raw_path, zip_filename)
		csv_filename = f'pulse{year}_puf_{week_num}.csv'
		out_filename = f'puf-{week_num}-slimmed.csv'
		output_file = os.path.join(slim_path, out_filename)

		# Skip existing files if we're not reprocessing
		if os.path.exists(output_file) and reprocess == False:
			print(f'   - Skipping ZIP file - it has already been parsed.')
			continue

		# Reprocess existing files if user said to.
		elif os.path.exists(output_file) and reprocess == True:
			print(f'   - Reprocessing existing ZIP file')


		# Extract the CSV from the ZIP and load it into an Agate table.
		print(f'   - Opening ZIP file')
		with ZipFile(zip_filepath) as zf:
			with zf.open(csv_filename, 'r') as csv_file:
				print(f'   - Parsing CSV')
				table = agate.Table.from_csv(
					io.TextIOWrapper(csv_file, 'utf-8'),
					column_types=agate.TypeTester(
						force={
						},
						types=[agate.Text()],
						# limit=100,
					),
				)
				# Sniff out the columns available in this set, so we know which ones to keep
				cols = cols_common
				if 'ENROLL1' in table.column_names:
					cols = cols + cols_1
				elif 'ENRPUBCHK' in table.column_names:
					cols = cols + cols_2
				# elif 'TENROLLPUB' in table.column_names:
				# 	cols = cols + cols_2
				if 'HWEIGHT' in table.column_names:
					cols = cols + ['HWEIGHT']
				if 'PWEIGHT' in table.column_names:
					cols = cols + ['PWEIGHT']

				# Now, limit the table to just the columns we care about
				print(f'   - Reducing columns')
				table = table.select(cols)

				# Save the table
				print(f'   - Saving slimmed table as CSV')
				table.to_csv(output_file)


if __name__ == "__main__":

	# Directory from which the script is running
	script_path = os.path.dirname(os.path.realpath(__file__))

	# Various data directories
	slim_path = os.path.join(script_path, 'data', 'slimmed')
	raw_path = os.path.join(script_path, 'data', 'raw')

	main(
		# Change to `False` if you want to perform the slim operation only on new, unprocessed files.
		reprocess=True,
	)
