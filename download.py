import re
import urllib.request
import os
from time import sleep

def download(url, output_file):
	try:
		result = urllib.request.urlopen(url).read()
	except Exception as e:
		print(e)
		return False

	if result:
		print(f'   - Saving to {output_file}')
		with open(output_file, 'wb') as f:
			f.write(result)
			return True

	return False


def main(urls, output_path, script_path):
	print(f'Using output_path: {output_path}')

	for url in urls:
		url_pieces = url.split('/')
		filename = url_pieces[-1]

		output_file = os.path.join(output_path, filename)

		if os.path.exists(output_file):
			print(f' * Skipping `{url}` - it is already downloaded.')
			continue

		print(f' * Fetching `{url}`')
		success = download(url, output_file)
		if success:
			print(f'   - Success!')
		else:
			print(f'   - Failed to download!')

		sleep(3)

	print('DONE!')




if __name__ == "__main__":

	# Directory from which the script is running
	script_path = os.path.dirname(os.path.realpath(__file__))

	# Various data directories
	output_path = os.path.join(script_path, 'data', 'raw')

	# Get the list of wanted PUF file URLs.
	urls = None
	try:
		with open(os.path.join(script_path, 'puf-urls.txt'), 'r') as f:
			urls = f.readlines()
			urls = [x.strip('\n').strip('\r').strip() for x in urls]
	except Exception as e:
		print(e)
		exit()

	main(
		urls=urls,
		output_path=output_path,
		script_path=script_path,
	)
