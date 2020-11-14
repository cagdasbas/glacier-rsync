import logging
import os
import shutil
import sqlite3
from contextlib import ExitStack
from datetime import datetime


class BackupUtil:
	def __init__(self, args):
		self.src = args.src
		self.compress_algo = args.compress
		self.remove_compressed = args.remove_compressed
		self.desc = args.desc

		self.vault = args.vault
		self.region = args.region

		self.db_file = args.db
		try:
			self.conn = sqlite3.connect(self.db_file)
			logging.info("connected to glacier rsync db")
		except sqlite3.Error as e:
			logging.error(f"Cannot create glacier rsync db: {str(e)}")
			raise ValueError(f"Cannot create glacier rsync db: {str(e)}")

		cur = self.conn.cursor()
		cur.execute(
			'create table if not exists sync_history (id integer primary key, path text, file_size integer, mtime float, archive_id text, timestamp text);')
		self.conn.commit()
		cur.close()
		logging.debug("init is done")

	def backup(self):
		file_list = []
		if os.path.isdir(self.src):
			for root, dirs, files in os.walk(self.src):
				for file in files:
					file_list.append(os.path.abspath(os.path.join(root, file)))
		else:
			file_list.append(self.src)

		logging.debug(f"number of files to backup: {len(file_list)}")
		for file in file_list:
			if not self._check_if_backed_up(file):
				logging.debug(f"{file} will be backed up")
				compressed_file = self._compress(file)
				logging.debug(f"{file} is compressed as {compressed_file}")
				archive_id = self._backup(compressed_file)
				logging.debug(f"{file} backed up with {archive_id}")
				self._mark_backed_up(file, archive_id)
				self._remove_file(compressed_file)
				logging.debug(f"{compressed_file} is deleted")
			else:
				logging.debug(f"{file} is already backed up, skipping...")

	def _check_if_backed_up(self, path):
		"""
		Check if file is already backed up
		:param file: full file path
		:return: True if file is backed up, False if file is not backed up
		"""
		file_size, mtime = self.__get_stats(path)
		cur = self.conn.cursor()
		cur.execute(
			f"select * from sync_history where path='{path}' and file_size={file_size} and mtime={mtime}")
		rows = cur.fetchall()
		return len(rows) > 0

	def _compress(self, file):
		"""
		Compress given file with given algorithm
		:param file: input file path
		:return: compressed file path. If no compression is selected, the same file path
		"""
		if self.compress_algo is None:
			return file

		if self.compress_algo == 'gzip':
			try:
				import gzip
			except ImportError:
				msg = "cannot import gzip. Please install required libraries!"
				logging.error(msg)
				raise ValueError(msg)

			compressed_file = f'{file}.gz'
			self.__compress_stream(open(file, 'rb'), gzip.open(compressed_file, 'wb'))

			return compressed_file
		elif self.compress_algo == 'zstd':
			try:
				import zstandard as zstd
			except ImportError:
				msg = "cannot import zstd. Please install required libraries!"
				logging.error(msg)
				raise ValueError(msg)

			compressed_file = f'{file}.zstd'
			cctx = zstd.ZstdCompressor()
			self.__compress_stream(open(file, 'rb'), cctx.stream_writer(open(compressed_file, 'wb')))

			return compressed_file
		else:
			return None

	def __compress_stream(self, input, output):
		"""
		Redirect input file to compressor output
		:param input: input stream
		:param output: output compressor stream
		"""
		with ExitStack() as stack:
			f_in = stack.enter_context(input)
			f_out = stack.enter_context(output)
			shutil.copyfileobj(f_in, f_out)

	def _backup(self, compressed_file):
		return "1234"

	def _remove_file(self, path):
		"""
		Delete the given file. File will not be deleted if compression is off because compressed_file == file
		:param path: absolute path of the file
		"""
		if self.compress_algo is None:
			return
		elif self.remove_compressed:
			os.remove(path)

	def _mark_backed_up(self, path, archive_id):
		"""
		Mark the given file as archived in db with associated information
		:param path: absolute path of the file
		:param archive_id: glacier archive id
		"""
		file_size, mtime = self.__get_stats(path)
		timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
		cur = self.conn.cursor()
		cur.execute(
			f"insert into sync_history (path, file_size, mtime, archive_id, timestamp) "
			f"values ('{path}', {file_size}, {mtime}, '{archive_id}', '{timestamp}')"
		)
		cur.close()
		self.conn.commit()

	def __get_stats(self, path):
		"""
		Get the stats of given file
		:param path: absolute path of the file
		:return: tuple(file size, modified time)
		"""
		return os.path.getsize(path), os.path.getmtime(path)
