from argparse import ArgumentParser


class ArgParser:

	def __init__(self):
		self.parser = ArgumentParser(description='Rsync like glacier backup util')
		self.parser.add_argument(
			"--loglevel",
			dest="log_level",
			type=str,
			choices=list(logging._levelToName.values()),
			default="INFO",
			help="log level"
		)
		self.parser.add_argument(
			'--vault',
			metavar='vault',
			help='Glacier vault name',
			required=True
		)
		self.parser.add_argument(
			'--region',
			metavar='region',
			help='Glacier region name',
			required=True
		)
		self.parser.add_argument(
			'--compress',
			help='Compression algorithm',
			choices=["gzip", "zstd"],
			default=None
		)
		self.parser.add_argument(
			'--remove_compressed',
			help='Remove the compressed file afterwards',
			action="store_true",
		)
		self.parser.add_argument(
			'--desc',
			metavar='desc',
			help='A description for the archive that will be stored in Amazon Glacer'
		)
		self.parser.add_argument(
			'src',
			metavar='src',
			help='file or folder to generate archive from'
		)

	def get_args(self):
		return self.parser.parse_args()
