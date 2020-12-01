import argparse
import logging


class ArgParser:

	def __init__(self):
		self.parser = argparse.ArgumentParser(description="Rsync like glacier backup util")
		self.parser.add_argument(
			"--loglevel",
			dest="log_level",
			type=str,
			choices=list(logging._levelToName.values()),
			default="INFO",
			help="log level"
		)
		self.parser.add_argument(
			"--db",
			metavar="db",
			help="database file to store sync info",
			default="glacier.db"
		)
		self.parser.add_argument(
			"--vault",
			metavar="vault",
			help="Glacier vault name",
			required=True
		)
		self.parser.add_argument(
			"--region",
			metavar="region",
			help="Glacier region name",
			required=True
		)
		self.parser.add_argument(
			"--compress",
			help="Enable compression. Only zstd is supported",
			type=self.str2bool,
			default=False
		)
		self.parser.add_argument(
			"--remove_compressed",
			help="Remove the compressed file afterwards",
			action="store_true",
		)
		self.parser.add_argument(
			"--desc",
			metavar="desc",
			help="A description for the archive that will be stored in Amazon Glacer"
		)
		self.parser.add_argument(
			"src",
			metavar="src",
			help="file or folder to generate archive from"
		)

	def str2bool(self, v):
		if isinstance(v, bool):
			return v
		if v.lower() in ('yes', 'true', 't', 'y', '1'):
			return True
		elif v.lower() in ('no', 'false', 'f', 'n', '0'):
			return False
		else:
			raise argparse.ArgumentTypeError('Boolean value expected.')

	def get_args(self):
		return self.parser.parse_args()
