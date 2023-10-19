import io
import os
import agate
import warnings

def get_school_year(row):
	if row['WEEK'] < 13:
		return '2019-2020'
	if row['WEEK'] < 33:
		return '2020-2021'
	if row['WEEK'] < 49:
		return '2021-2022'
	if row['WEEK'] < 61:
		return '2022-2023'
	if row['WEEK'] >= 61:
		return '2023-2024'
	return None

def get_no_ans(row):
	value = 0
	if 'tot_none_99' in row.keys():
		value += row['tot_none_99']
	if 'tot_none_88' in row.keys():
		value += row['tot_none_88']
	return value


def main(out_path, wanted_files=[]):
	tables = []

	for file_path in wanted_files:
		print(f'* Loading {file_path}')

		table = agate.Table.from_csv(
			file_path,
			column_types=agate.TypeTester(
				force={
					'WEEK': agate.Number(),
					'SCRAM': agate.Text(),
					'EST_ST': agate.Number(),
					'ENROLL1': agate.Number(),
					'ENROLL2': agate.Number(),
					'ENROLL3': agate.Number(),
					'ENRPUBCHK': agate.Number(),
					'ENRPRVCHK': agate.Number(),
					'ENRHMSCHK': agate.Number(),
					'TENROLLPUB': agate.Number(),
					'TENROLLPRV': agate.Number(),
					'TENROLLHMSCH': agate.Number(),
					'ENROLLNONE': agate.Number(),
					'PWEIGHT': agate.Number(),
					'HWEIGHT': agate.Number(),
					'TBIRTH_YEAR': agate.Number(),
					'THHLD_NUMKID': agate.Number(),
					'INCOME': agate.Number(),
					'EEDUC': agate.Number(),
					'RRACE': agate.Number(),
					'RHISPANIC': agate.Number(),
				},
				types=[agate.Text()],
				# limit=100,
			),
		)

		tables.append(table)

	print(f'* Merging tables')
	table = agate.Table.merge(tables)

	table = table.compute([
		('SCHYR', agate.Formula(agate.Text(), get_school_year)),
	])

	# Calculate total number of all households for state/national. Will join this back to school-limited data later.
	natl_tot = table.group_by('SCHYR').aggregate([('HHD_ALL', agate.Sum('HWEIGHT'))])
	state_tot = table.group_by('SCHYR').group_by('EST_ST').aggregate([('HHD_ALL', agate.Sum('HWEIGHT'))])

	# Otherwise, we need to limit the data to the universe of households where there are kids in some type of school.
	# Originally I was doing this with a `ENRxxxCHK == 1` criteria. But Census Bureau's tables use `TENROLLxxx > 0`.
	sch_table = table.where(lambda row: row['TENROLLPUB'] > 0 or row['TENROLLPRV'] > 0 or row['TENROLLHMSCH'] > 0)

	# The Census Bureau also filters with `TBIRTH_YEAR <= 2005` and `THHLD_NUMKID > 0`,
	# though I have found these usually have very little effect.
	sch_table = sch_table.where(lambda row: row['TBIRTH_YEAR'] <= 2005 and row['THHLD_NUMKID'] > 0)

	sch_table = sch_table.compute([
		# Compute household totals for each type of school enrollment, using Census Bureau's HWEIGHT
		('HHD_ENRPUB', agate.Formula(agate.Number(), lambda row: row['HWEIGHT'] if row['TENROLLPUB'] > 0 else 0 )),
		('HHD_ENRPRV', agate.Formula(agate.Number(), lambda row: row['HWEIGHT'] if row['TENROLLPRV'] > 0 else 0 )),
		('HHD_ENRHMS', agate.Formula(agate.Number(), lambda row: row['HWEIGHT'] if row['TENROLLHMSCH'] > 0 else 0 )),
		# Compute student totals for each type of school enrollment. I'm using HWEIGHT because it's the best available weight.
		# But researchers caution this weighting has flaws when used for the student numbers. Results should be considered approximations.
		('STU_ENRPUB', agate.Formula(agate.Number(), lambda row: row['TENROLLPUB'] * row['HWEIGHT'] if row['TENROLLPUB'] > 0 else 0 )),
		('STU_ENRPRV', agate.Formula(agate.Number(), lambda row: row['TENROLLPRV'] * row['HWEIGHT'] if row['TENROLLPRV'] > 0 else 0 )),
		('STU_ENRHMS', agate.Formula(agate.Number(), lambda row: row['TENROLLHMSCH'] * row['HWEIGHT'] if row['TENROLLHMSCH'] > 0 else 0 )),
	])

	# Calculate total number of students in the survey enrolled in any type of schooling.
	sch_table = sch_table.compute([
		('STU_ENRANY', agate.Formula(agate.Number(), lambda row: row['STU_ENRPUB'] + row['STU_ENRPRV'] + row['STU_ENRHMS'] )),
	])

	# Begin grouping and aggregating the data.
	# We'll run through essentially the same steps at first the national level, and then the state level.
	for scope in ['national', 'states']:
		# Group by school year.
		this_table = (sch_table
			.group_by('SCHYR')
		)
		if scope == 'states':
			# If this is the state-level analysis, then we also need to group by state number.
			this_table = (this_table
				.group_by('EST_ST')
			)
		this_table = (this_table
			.aggregate([
				# This is the total number of households with kids in any type of school.
				('HHD_ENRANY', agate.Sum('HWEIGHT')),
				# These are the total households by type of school
				('HHD_ENRPUB', agate.Sum('HHD_ENRPUB')),
				('HHD_ENRPRV', agate.Sum('HHD_ENRPRV')),
				('HHD_ENRHMS', agate.Sum('HHD_ENRHMS')),
				# These are the total number of *students* by type of school.
				('STU_ENRANY', agate.Sum('STU_ENRANY')),
				('STU_ENRPUB', agate.Sum('STU_ENRPUB')),
				('STU_ENRPRV', agate.Sum('STU_ENRPRV')),
				('STU_ENRHMS', agate.Sum('STU_ENRHMS')),
			])
		)

		if scope == 'states':
			this_table = state_tot.join(this_table, left_key=['SCHYR','EST_ST'])
		else:
			this_table = natl_tot.join(this_table, left_key=['SCHYR'])

		# Now that we have aggregated the totals at either the national or state levels, let's calculate percentages.
		this_table = (this_table
			.compute([
				# Percentages of households with kids in any type of school
				('HHD_PCTPUB', agate.Formula(agate.Number(), lambda row: row['HHD_ENRPUB'] / row['HHD_ENRANY'] if row['HHD_ENRANY'] > 0 else 0 )),
				('HHD_PCTPRV', agate.Formula(agate.Number(), lambda row: row['HHD_ENRPRV'] / row['HHD_ENRANY'] if row['HHD_ENRANY'] > 0 else 0 )),
				('HHD_PCTHMS', agate.Formula(agate.Number(), lambda row: row['HHD_ENRHMS'] / row['HHD_ENRANY'] if row['HHD_ENRANY'] > 0 else 0 )),
				# Percentage of ALL households
				('STU_PCTPUB', agate.Formula(agate.Number(), lambda row: row['STU_ENRPUB'] / row['STU_ENRANY'] if row['STU_ENRANY'] > 0 else 0 )),
				('STU_PCTPRV', agate.Formula(agate.Number(), lambda row: row['STU_ENRPRV'] / row['STU_ENRANY'] if row['STU_ENRANY'] > 0 else 0 )),
				('STU_PCTHMS', agate.Formula(agate.Number(), lambda row: row['STU_ENRHMS'] / row['STU_ENRANY'] if row['STU_ENRANY'] > 0 else 0 )),
			])
		)

		if scope == 'states':
			this_table = this_table.order_by('SCHYR').order_by('EST_ST')

		# Save final CSV
		this_table.to_csv(os.path.join(out_path, f'analyzed-{scope}.csv'))



def demographic_analysis(out_path, wanted_files=[]):
	tables = []

	for file_path in wanted_files:
		print(f'* Loading {file_path}')

		table = agate.Table.from_csv(
			file_path,
			column_types=agate.TypeTester(
				force={
					'WEEK': agate.Number(),
					'SCRAM': agate.Text(),
					'EST_ST': agate.Number(),
					'ENROLL1': agate.Number(),
					'ENROLL2': agate.Number(),
					'ENROLL3': agate.Number(),
					'ENRPUBCHK': agate.Number(),
					'ENRPRVCHK': agate.Number(),
					'ENRHMSCHK': agate.Number(),
					'TENROLLPUB': agate.Number(),
					'TENROLLPRV': agate.Number(),
					'TENROLLHMSCH': agate.Number(),
					'ENROLLNONE': agate.Number(),
					'PWEIGHT': agate.Number(),
					'HWEIGHT': agate.Number(),
					'TBIRTH_YEAR': agate.Number(),
					'THHLD_NUMKID': agate.Number(),
					'INCOME': agate.Number(),
					'EEDUC': agate.Number(),
					'RRACE': agate.Number(),
					'RHISPANIC': agate.Number(),
				},
				types=[agate.Text()],
				# limit=100,
			),
		)

		tables.append(table)

	print(f'* Merging tables')
	table = agate.Table.merge(tables)

	# Determine the school year based on what phase the PUF file is from
	table = table.compute([
		('SCHYR', agate.Formula(agate.Text(), get_school_year)),
	])

	# Limit the data we use to just the current school year.
	base_table = table.where(lambda row: row['SCHYR'] == '2023-2024')

	# Next, limit the data to the universe of households where there are kids in some type of school.
	# Originally I was doing this with a `ENRxxxCHK == 1` criteria. But Census Bureau's tables use `TENROLLxxx > 0`.
	base_table = base_table.where(lambda row: row['TENROLLPUB'] > 0 or row['TENROLLPRV'] > 0 or row['TENROLLHMSCH'] > 0)

	# The Census Bureau also filters with `TBIRTH_YEAR <= 2005` and `THHLD_NUMKID > 0`,
	# though I have found these usually have very little effect.
	base_table = base_table.where(lambda row: row['TBIRTH_YEAR'] <= 2005 and row['THHLD_NUMKID'] > 0)

	# Compute the weighted households that have any kids in each type of school.
	base_table = base_table.compute([
		('HHD_ENRPUB', agate.Formula(agate.Number(), lambda row: row['HWEIGHT'] if row['TENROLLPUB'] > 0 else 0 )),
		('HHD_ENRPRV', agate.Formula(agate.Number(), lambda row: row['HWEIGHT'] if row['TENROLLPRV'] > 0 else 0 )),
		('HHD_ENRHMS', agate.Formula(agate.Number(), lambda row: row['HWEIGHT'] if row['TENROLLHMSCH'] > 0 else 0 )),
	])

	# Define the category labels we're going to use in each analysis.
	analyses = [
		{
			'field': 'INCOME',
			'categories': [
				'0-24',
				'25-34',
				'35-49',
				'50-74',
				'75-99',
				'100-149',
				'150-199',
				'200-up',
			],
			'filename': 'income-comparison',
		},
		{
			'field': 'EEDUC',
			'categories': [
				'lt_hs',
				'some_hs',
				'hs_grad',
				'some_col',
				'deg_ass',
				'deg_bac',
				'deg_grad',
			],
			'filename': 'education-comparison',
		},
		{
			'field': 'RRACE',
			'categories': [
				'white',
				'black',
				'asian',
				'other',
			],
			'filename': 'race-comparison',
		},
		{
			'field': 'RHISPANIC',
			'categories': [
				'not-hispanic',
				'hispanic',
			],
			'filename': 'hispanic-comparison',
		},
	]


	# Iterate over the types of analyses.
	for analysis in analyses:
		print(f'* Analyzing data based on `{analysis["field"]}` column')

		# Iterate over each scope
		for scope in ['national', 'states']:
			print(f'  + Calculating data for {scope}')

			all_tables = []

			# Iterate over each school type
			for sch_type in ['PUB','PRV', 'HMS']:
				print(f'    - Calculating data for {sch_type}')

				sch_type_table = base_table.where(lambda row: row[f'HHD_ENR{sch_type}'] > 0)

				if scope == 'states':
					sch_type_table = sch_type_table.group_by('EST_ST')

				# Group by the demographic field, then calculate a weighted total of households for each school type.
				sch_type_table = (sch_type_table
					.group_by(analysis['field'])
					.aggregate([
						('COUNT', agate.Sum('HWEIGHT')),
					])
					.compute([
						('SCHTYPE', agate.Formula(agate.Text(), lambda row: sch_type)),
					])
				)

				# Reconfigure the table so demographic categories become columns.
				denormalize_key = ['SCHTYPE']
				if scope == 'states':
					denormalize_key = ['EST_ST'] + denormalize_key

				sch_type_table = sch_type_table.denormalize(
					key=denormalize_key,
					property_column=analysis['field'],
					value_column='COUNT',
				)

				# Set up the renames and the final column order for this particular analysis.
				renames = {f'{i+1}': f'tot_{cat}' for i, cat in enumerate(analysis['categories'])}
				final_columns = ['SCHTYPE'] + [f'tot_{cat}' for cat in analysis['categories']]

				# The -88 and -99 columns don't always show up, so to avoid errors when renaming, we'll only include these if we need them.
				if '-99' in sch_type_table.column_names:
					renames.update({'-99': 'tot_none_99'})
					final_columns = final_columns + ['tot_none_99']
				if '-88' in sch_type_table.column_names:
					renames.update({'-88': 'tot_none_88'})
					final_columns = final_columns + ['tot_none_88']

				if scope == 'states':
					final_columns = ['EST_ST'] + final_columns

				# Rename the columns from the survey values (1, 2, 3) to semi-human-understandstable (`tot_0-25`, `tot_25-49`, etc)
				sch_type_table = sch_type_table.rename(renames)

				# Rearrange the columns in the order we want.
				sch_type_table = sch_type_table.select(final_columns)

				all_tables.append(sch_type_table)

			print(f'    - Merging data for all school types')

			# Combine the school type tables into a single table, then sort everything into a convenient order
			master_table = agate.Table.merge(all_tables)

			master_table = master_table.order_by('SCHTYPE')
			if scope == 'states':
				master_table = master_table.order_by('EST_ST')

			# Compute totals and percentages of answered / not-answered
			master_table = master_table.compute([
				('tot_ans', agate.Formula(agate.Number(), lambda row: sum([row[f'tot_{cat}'] for cat in analysis['categories']]))),
			])
			master_table = master_table.compute([
				('tot_no_ans', agate.Formula(agate.Number(), get_no_ans)),
			])
			master_table = master_table.compute([
				('tot', agate.Formula(agate.Number(), lambda row: row['tot_no_ans'] + row['tot_ans'])),
			])
			master_table = master_table.compute([
				('pct_ans', agate.Formula(agate.Number(), lambda row: row['tot_ans'] / row['tot'] if row['tot'] > 0 else 0 )),
			])

			# Compute percentages for each category (e.g. `tot_0-25`)
			for cat in analysis['categories']:
				master_table = master_table.compute([
					(f'pct_{cat}', agate.Formula(agate.Number(), lambda row: row[f'tot_{cat}'] / row['tot_ans'] if row['tot_ans'] > 0 else 0 )),
				])

			# Save the final analysis.
			print(f'    - Saving results to {analysis["filename"]}-{scope}.csv')
			master_table.to_csv(os.path.join(out_path, f'{analysis["filename"]}-{scope}.csv'))




if __name__ == "__main__":

	# Ignore the RuntimeWarning about tables that don't contain certain columns.
	warnings.filterwarnings('ignore', category=RuntimeWarning)

	# Directory from which the script is running
	script_path = os.path.dirname(os.path.realpath(__file__))

	# Various data directories
	slim_path = os.path.join(script_path, 'data', 'slimmed')
	out_path = os.path.join(script_path, 'data', 'analyzed')

	# Call the main function, which will perform the basic analysis
	# to calculate household and student totals for each school type.
	main(
		out_path=out_path,
		wanted_files=[
			os.path.join(slim_path, 'puf-28-slimmed.csv'),
			os.path.join(slim_path, 'puf-43-slimmed.csv'),
			os.path.join(slim_path, 'puf-49-slimmed.csv'),
			os.path.join(slim_path, 'puf-62-slimmed.csv'),
		],
	)

	# Calculate household totals for various economic and demographic characteristics.
	demographic_analysis(
		out_path=out_path,
		wanted_files=[
			os.path.join(slim_path, 'puf-62-slimmed.csv'),
		],
	)

