# !/usr/bin/env python

import logging

from glacier_rsync.argparser import ArgParser
from glacier_rsync.backup_util import BackupUtil


def main():
	args = ArgParser().get_args()
	logging.getLogger(__name__)
	logging.basicConfig(
		format="%(asctime)s - %(module)s.%(funcName)s:%(lineno)d - %(levelname)s - %(message)s",
		level=getattr(logging, args.log_level, None))

	backup_util = BackupUtil(args)
	backup_util.backup()


if __name__ == "__main__":
	main()
