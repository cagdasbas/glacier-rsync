import binascii
import hashlib
import logging
import os
import sqlite3
import sys

import boto3
from botocore.exceptions import ClientError

from glacier_rsync.file_cache import FileCache


class BackupUtil:
	def __init__(self, args):
		self.src = args.src
		self.compress = args.compress
		self.remove_compressed = args.remove_compressed
		self.desc = args.desc
		self.part_size = args.partsize

		self.vault = args.vault
		self.region = args.region

		self.glacier = boto3.client("glacier", region_name=self.region)

		self.db_file = args.db
		try:
			self.conn = sqlite3.connect(self.db_file)
			logging.info("connected to glacier rsync db")
		except sqlite3.Error as e:
			logging.error(f"Cannot create glacier rsync db: {str(e)}")
			raise ValueError(f"Cannot create glacier rsync db: {str(e)}")

		cur = self.conn.cursor()
		try:
			cur.execute(
				"create table if not exists sync_history (id integer primary key, path text, file_size integer, mtime float, archive_id text, location text, checksum text, compression text, timestamp text);")
			self.conn.commit()
		except sqlite3.OperationalError as e:
			logging.error(f"DB error. Cannot mark the file as backed up: {str(e)})")
			sys.exit(2)
		finally:
			cur.close()
		logging.debug("init is done")

	def backup(self):
		"""
		Interface function to find files and apply logic
		"""
		file_list = []
		if os.path.isdir(self.src):  # if the source is a directory find all the files
			for root, dirs, files in os.walk(self.src):
				for file in files:
					file_list.append(os.path.abspath(os.path.join(root, file)))
		else:
			file_list.append(self.src)  # if the source is a file just process it

		logging.debug(f"number of files to backup: {len(file_list)}")
		for file in file_list:
			if not self._check_if_backed_up(file):  # True if already backed up
				logging.debug(f"{file} will be backed up")
				file_object, compressed_file_object = self._compress(file)  # compress the file if specified
				logging.debug(f"{file} is compressed as")
				archive = self._backup(compressed_file_object)
				file_object.close()
				self._mark_backed_up(file, archive)
			else:
				logging.debug(f"{file} is already backed up, skipping...")

	def _check_if_backed_up(self, path):
		"""
		Check if file is already backed up
		:param file: full file path
		:return: True if file is backed up, False if file is not backed up
		"""
		file_size, mtime = self.__get_stats(path)  # file size and mtime should match. if not it will be backed up again
		cur = self.conn.cursor()
		try:
			cur.execute(
				f"select * from sync_history where path='{path}' and file_size={file_size} and mtime={mtime}")
			rows = cur.fetchall()
		except sqlite3.OperationalError as e:
			logging.error(f"DB error. Cannot mark the file as backed up: {str(e)})")
			sys.exit(3)
		finally:
			cur.close()
		return len(rows) > 0

	def _compress(self, file):
		"""
		Compress given file with given algorithm
		:param file: input file path
		:return: compressed file path. If no compression is selected, the same file path
		"""

		file_object = open(file, 'rb')
		compression = False

		if self.compress:
			try:
				import zstandard as zstd
			except ImportError:
				msg = "cannot import zstd. Please install required libraries!"
				logging.error(msg)
				raise ValueError(msg)
			compression = True

		return file_object, FileCache(file_object.readable(), compression=compression)

	def calculate_tree_hash(self, part, part_size):
		"""
		Calculate hash of single part
		:param part: data chunk
		:param part_size: size of the chunk
		:return: calculated hash
		"""
		checksums = []
		upper_bound = min(len(part), part_size)
		step = 1024 * 1024  # 1 MB
		for chunk_pos in range(0, upper_bound, step):
			chunk = part[chunk_pos: chunk_pos + step]
			checksums.append(hashlib.sha256(chunk).hexdigest())
			del chunk
		return self.calculate_total_tree_hash(checksums)

	def calculate_total_tree_hash(self, checksums):
		"""
		Calculate hash of a list
		:param checksums: list(checksum) -> a list of checksum
		:return: total calculated hash
		"""
		tree = checksums[:]
		while len(tree) > 1:
			parent = []
			for i in range(0, len(tree), 2):
				if i < len(tree) - 1:
					part1 = binascii.unhexlify(tree[i])
					part2 = binascii.unhexlify(tree[i + 1])
					parent.append(hashlib.sha256(part1 + part2).hexdigest())
				else:
					parent.append(tree[i])
			tree = parent
		return tree[0]

	def _backup(self, src_file_object):
		"""
		Send the file to glacier
		:param src_file_object: FileCache object
		:return: archive information
		"""
		if src_file_object is None:  # only happens if unsupported compression algorithm
			return None
		try:
			response = self.glacier.initiate_multipart_upload(vaultName=self.vault, partSize=str(self.part_size))
			upload_id = response['uploadId']

			byte_pos = 0
			list_of_checksums = []
			while True:
				chunk = src_file_object.read(self.part_size)
				if chunk is None:
					break
				range_header = "bytes {}-{}/*".format(
					byte_pos, byte_pos + len(chunk) - 1
				)
				print(range_header)
				byte_pos += len(chunk)
				response = self.glacier.upload_multipart_part(
					vaultName=self.vault,
					uploadId=upload_id,
					range=range_header,
					body=chunk,
				)
				checksum = response["checksum"]
				list_of_checksums.append(checksum)

			total_tree_hash = self.calculate_total_tree_hash(list_of_checksums)
			archive = self.glacier.complete_multipart_upload(
				vaultName=self.vault,
				uploadId=upload_id,
				archiveSize=str(byte_pos),
				checksum=total_tree_hash,
			)
		except ClientError as e:
			logging.error(e)
			return None

		# Return dictionary of archive information
		return archive

	def _mark_backed_up(self, path, archive):
		"""
		Mark the given file as archived in db with associated information
		:param path: absolute path of the file
		:param archive_id: glacier archive id
		"""
		if archive is None:
			logging.error(f"{path} cannot be backed up")
			return
		archive_id = archive['archiveId']
		location = archive['location']
		checksum = archive['checksum']
		timestamp = archive['ResponseMetadata']['HTTPHeaders']['date']
		compression = "plain"
		if self.compress:
			compression = "zstd"

		file_size, mtime = self.__get_stats(path)
		cur = self.conn.cursor()
		try:
			cur.execute(
				f"insert into sync_history (path, file_size, mtime, archive_id, location, checksum, compression, timestamp) "
				f"values ('{path}', {file_size}, {mtime}, '{archive_id}', '{location}', '{checksum}', '{compression}', '{timestamp}')"
			)
			self.conn.commit()
		except sqlite3.OperationalError as e:
			logging.error(f"DB error. Cannot mark the file as backed up: {str(e)})")
			sys.exit(1)  # cannot continue if cannot mark
		finally:
			cur.close()

	def __get_stats(self, path):
		"""
		Get the stats of given file
		:param path: absolute path of the file
		:return: tuple(file size, modified time)
		"""
		return os.path.getsize(path), os.path.getmtime(path)
