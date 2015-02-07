import os
import re
import threading
import logging as log
import time
import sys
import heapq

'''
@author: Vikram Somu

The program starts with a thread that executes the main method found
towards the bottom of this file. This thread ends up being the point of entry
for both the single directory search thread and the multiple file processing 
worker threads that get spawned.
'''

# Thread to recursively search the file system tree
class DirectorySearchThread(threading.Thread):
	def __init__(self, path, worker_pool):
		super(DirectorySearchThread, self).__init__()
		self.path = path
		self.worker_pool = worker_pool

	def run(self):
		if not os.path.isdir(self.path):
			raise Exception("%s is not a valid directory" % self.path)
		else:
			self.__find_txt_files(self.path)

	def __find_txt_files(self, path):
		for fname in os.listdir(path):
			# log.debug('found file: ' + fname)
			f_path = os.path.join(path, fname)
			if os.path.isdir(f_path):
				self.__find_txt_files(f_path)
			else:
				# Hand file path off to worker pool if txt file
				if re.search('(.txt$)', fname):
					self.worker_pool.add_file(f_path)


# Worker thread to process a text file
class FileProcessingThread(threading.Thread):
	def __init__(self, tracker, worker_pool):
		super(FileProcessingThread, self).__init__()
		self.word_tracker = tracker
		self.worker_pool = worker_pool

	def run(self):
		# Keep the thread alive while we still have file jobs in the queue
		while self.worker_pool.file_queue:
			fp = self.worker_pool.file_queue.pop(0)
			self.__parse_file(fp)
		self.worker_pool.notify_thread_finished()

	'''
	Parses every line of the file, using the word 
	generator to add words to the tracker.
	'''
	def __parse_file(self, fp):
		if not os.path.isfile(fp):
			raise Exception("%s is not a valid file" % fp)

		with open(fp, 'r') as f:
			log.debug('parsing text file: %s' % fp)

			for line in f:
				wgen = self.__word_gen(line.strip())

				for word in wgen:
					self.word_tracker.add_instance(word) # Add the word instance to the tracker


	# Generator to parse out words from a line of text
	def __word_gen(self, line):
		non_delim = "([A-Za-z0-9])"
		start = 0
		pos = 0

		while pos < len(line):
			while pos < len(line) and re.match(non_delim, line[pos]):
				pos += 1

			word = line[start:pos]
			if len(word) > 1: 
				yield word
			start = pos + 1
			pos = start


class WorkerThreadPool(object):
	'''
	cb is an optional callback argument to be executed upon endstate
	you could also have nothing execute upon endstate
	'''
	def __init__(self, max_threads, tracker, cb=lambda: None):
		self.tracker = tracker
		self.max_threads = max_threads
		self.file_queue = []
		self.live_threads = 0
		self.awaiting_endstate = False
		self.endstate_cb = cb
		self.lock = threading.Lock()

	# Enqueue file path, and attempt to allocate a thread
	def add_file(self, fp):
		log.debug('Added file to Queue: %s' % fp)
		self.file_queue.append(fp)
		self.__attempt_alloc_thread()		

	# Sets the pool's status to awaiting_endstate = True
	def wait_for_endstate(self):
		self.awaiting_endstate = True
		if self.__is_endstate():
			self.endstate_cb()
			

	'''
	Thread safely decrement live_threads, then check for end state 
	to see if self.endstate_cb() needs to be called; self.awaiting_endstate == True 
	only after wait_for_endstate() gets called
	'''
	def notify_thread_finished(self):
		self.__dec_threads()
		if self.__is_endstate():
			self.endstate_cb()

	'''
	End state where all worker threads are terminated 
	and no files are left in the queue
	'''
	def __is_endstate(self):
		return self.awaiting_endstate and self.live_threads == 0 \
			and not self.file_queue

	'''
	Attempt to allocate a thread if we have live_threads < max_threads
	increments live_threads thread safely
	'''
	def __attempt_alloc_thread(self):
		with self.lock:
			if self.live_threads < self.max_threads:
				self.live_threads += 1
				FileProcessingThread(self.tracker, self).start()

	# Decrements live_threads count in thread safe way
	def __dec_threads(self):
		with self.lock:
			self.live_threads -= 1
			


class WordFreqTracker(object):
	def __init__(self):
		self.master_counts = {}
		self.top_ten = []
		self.num_uniq_words = 0
		self.lock = threading.Lock()

	# Prints top 10 words at the end
	def print_top10(self):
		print "Top 10 Most Frequent Words"
		inorder_ten = []
		ten_copy = self.top_ten[:]

		while ten_copy:
			inorder_ten.append(heapq.heappop(ten_copy))

		for i, e in enumerate(reversed(inorder_ten)):
			print '%i. %s: %i' % (i+1, e[1], e[0])

	# Add an instance to the word freq tracker (thread safe)
	def add_instance(self, word):
		word = word.lower()

		with self.lock:
			if word in self.master_counts:
				self.master_counts[word] += 1
			else:
				self.master_counts[word] = 1
				self.num_uniq_words += 1
			
			self.__manage_top10(word)

	# Adds a candidate word to top 10 if appropriate
	def __manage_top10(self, candidate):
		candidate_freq = self.master_counts[candidate]
		in_top10 = False

		for i, e in enumerate(self.top_ten): # Check if our candidate is in the top 10 already
			if e[1] == candidate:
				self.top_ten[i] = (candidate_freq, candidate)
				heapq.heapify(self.top_ten)
				in_top10 = True

		if not in_top10:
			if len(self.top_ten) < 10: # Add to the top 10 until we have 10 words
				heapq.heappush(self.top_ten, (candidate_freq, candidate))
				# log.debug("TT Len: %s NumWord: %s" % (str(len(self.top_ten)), self.num_uniq_words))
			else: # Evict lowest frequent word for candidate word if necessary
				if self.top_ten[0][0] < candidate_freq:
					heapq.heappop(self.top_ten)
					heapq.heappush(self.top_ten, (candidate_freq, candidate))


def main(max_threads):
	tracker = WordFreqTracker()
	thread_pool = WorkerThreadPool(max_threads, tracker, tracker.print_top10)

	path = raw_input("Please enter a path to search:")
	dst = DirectorySearchThread(path, thread_pool)
	dst.start()

	'''
	Wait for the directory search thread to terminate
	before letting thread pool wait for endstate
	'''
	dst.join()
	# Now the thread pool awaits its endstate
	thread_pool.wait_for_endstate()


if __name__ == '__main__':
	sys.stdout.flush()
	log.basicConfig(level=log.DEBUG,
                    format='%(asctime)s [%(levelname)s] (%(threadName)-10s) %(message)s')
	# Set the max # of worker threads
	max_threads = 3
	main(max_threads)


