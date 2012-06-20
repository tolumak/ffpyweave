#!/usr/bin/env python
#coding: utf8

# pyweave - A single-file mozilla weave server.
# Alberto Bertogli (albertito@blitiri.com.ar)

#
# Configuration section
#
# You can edit these values, or create a file named "config.py" and put them
# there to make updating easier. The ones in config.py take precedence.
#

# Directory where entries are stored. Do *NOT* put it in an http-accessible
# path, as that will leak information. Put it in a separate path and make it
# read and writeable by the httpd user.
data_path = "/var/weave/data/"

# Server path. Path to the pyweave.cgi script starting from DocumentRoot. 
server_path = "/pyweave/pyweave.cgi/"

#
# End of configuration
# DO *NOT* EDIT ANYTHING PAST HERE
#


import sys
import os
import time
import cgi
import fcntl
import string

try:
	import cPickle as pickle
except ImportError:
	import pickle

try:
	import json
except ImportError:
	try:
		import simplejson as json
	except ImportError:
		print "Error: json (or simplejson) module is needed"
		sys.exit(1)


# Load the config file, if there is one
try:
	from config import data_path
except:
	pass


#
# Storage backend
#
# The storage is very simple, using Python's pickle for serialization of
# metadata and the filesystem for general storage. We have an intermediate
# collection class to make things more friendlier to use, but it has intimate
# knowledege of the backend.
#
# Note that it does not concern about users, as each user has his/her own
# independant storage.

# TODO: validate parentid, previd exist on store, and that no children exist
# on delete (?)

def fsencode(s):
	"""Encodes the string s so it's safe to use as a file name (i.e. no
	'/') and easy to read (otherwise we could just use base64)."""
	ns = s.replace('%', '%P')
	ns = ns.replace('/', '%S')
	return ns

def fsdecode(s):
	"Opposite of fsencode()."
	# Note that the order is the reverse of the fsencode() operations
	ns = s.replace('%S', '/')
	ns = ns.replace('%P', '%')
	return ns


COL_PREFIX = "collection-"
PAYLOAD_PREFIX = "payload-"

def col_path(col_id):
	"Returns the path component corresponding to the given collection id."
	return COL_PREFIX + fsencode(col_id)

def payload_path(wbo_id):
	return PAYLOAD_PREFIX + fsencode(wbo_id)

class dummy (object):
	pass

class WBO (object):
	def __init__(self, id, parent_id = None, pred_id = None, modified = 0,
			sortidx = 0, payload = None):
		# We must be careful with what we put here as it will be
		# pickled. That means no absolute paths (so the root data path
		# can be changed).
		self.id = id
		self.parent_id = parent_id
		self.pred_id = pred_id
		self.modified = modified
		self.sortidx = sortidx
		self.payload = payload

	def __getstate__(self):
		"""Called by pickle.dump() to ask us what we need pickled. We
		use it to remove the payload, to prevent it from being
		pickled by accident."""
		d = dict(self.__dict__)
		d['payload'] = None
		return d

	def load_payload(self, basepath):
		p = basepath + '/' + payload_path(self.id)
		if os.path.exists(p):
			self.payload = open(p).read()
		else:
			self.payload = None

	def save_payload(self, basepath):
		p = basepath + '/' + payload_path(self.id)
		open(p + '.tmp', 'w').write(self.payload)
		os.rename(p + '.tmp', p)

	def to_dict(self, basepath):
		self.load_payload(basepath)
		return {
			'id': self.id,
			'parentid': self.parent_id,
			'predecessorid': self.pred_id,
			'modified': self.modified,
			'sortindex': self.sortidx,
			'payload': self.payload
		}

	def from_dict(self, d, mtime = None):
		if 'parentid' in d:
			self.parent_id = d['parentid']
		if 'predecessorid' in d:
			self.pred_id = d['predecessorid']
		if mtime is not None:
			self.modified = mtime
		elif 'modified' in d:
			self.modified = d['modified']
		else:
			self.modified = time.time()
		if 'sortindex' in d:
			self.sortidx = d['sortindex']
		if 'payload' in d:
			self.payload = d['payload']

class Storage (object):
	def __init__(self, basepath):
		self.basepath = basepath

	def get_collection(self, id, create = False):
		colp = self.basepath + '/' + col_path(id)
		if not os.path.exists(colp):
			if create:
				os.mkdir(colp)
			else:
				raise KeyError
		return Collection(self.basepath, id)

	def new_collection(self, id):
		os.mkdir(self.basepath + '/' + col_path(id))
		return Collection(self.basepath, id)

	def list_collections(self):
		cs = []
		for d in os.listdir(self.basepath):
			if not d.startswith(COL_PREFIX):
				continue
			cs.append(Collection(self.basepath,
					d[len(COL_PREFIX):]))
		return cs

class Collection (object):
	def __init__(self, basepath, id):
		self.basepath = basepath + '/' + col_path(id)
		self.mtime = 0
		if os.path.exists(self.basepath):
			self.mtime = os.stat(self.basepath).st_mtime
		self.id = id
		self._wbos = None

	@property
	def wbos(self):
		if self._wbos is None:
			self.load_wbos()
		return self._wbos

	def load_wbos(self):
		if os.path.exists(self.basepath + '/wbo.db'):
			self._wbos = pickle.load(open(self.basepath + '/wbo.db'))
		else:
			self._wbos = {}

	def save_wbos(self):
		if not os.path.exists(self.basepath):
			os.mkdir(self.basepath)
		tp = self.basepath + '/wbo.db' + '.tmp'
		pickle.dump(self.wbos, open(tp, 'w'), pickle.HIGHEST_PROTOCOL)
		os.rename(tp, self.basepath + '/wbo.db')

	def put_wbo_json(self, wid, json_obj, ts = None):
		if ts is None:
			ts = time.time()

		if wid in self.wbos:
			wbo = self.wbos[wid]
		else:
			wbo = WBO(wid)
			self.wbos[wbo.id] = wbo
		wbo.from_dict(json_obj, ts)
		wbo.save_payload(self.basepath)
		return wbo.modified

	def list_wbos(self, ids = None, pred_id = None, parent_id = None,
			older = None, newer = None, full = None,
			idx_above = None, idx_below = None, limit = None,
			offset = None, sort = None):
		ws = self.wbos.values()

		if ids is not None:
			ws = [ w for w in ws if w.id in ids ]
		if pred_id is not None:
			ws = [ w for w in ws if w.pred_id == pred_id ]
		if parent_id is not None:
			ws = [ w for w in ws if w.parent_id == parent_id ]
		if older is not None:
			ws = [ w for w in ws if w.modified < older ]
		if newer is not None:
			ws = [ w for w in ws if w.modified > newer]
		if idx_above is not None:
			ws = [ w for w in ws if w.sortidx > idx_above ]
		if idx_below is not None:
			ws = [ w for w in ws if w.sortidx < idx_below ]

		if limit is not None:
			ws = ws[:limit]
		if offset is not None:
			ws = ws[offset:]

		if sort == 'oldest':
			ws.sort(cmp = \
				lambda x, y: cmp(x.modified, y.modified))
		elif sort == 'newest':
			ws.sort(reverse = True, cmp = \
				lambda x, y: cmp(x.modified, y.modified))
		elif sort == 'index':
			ws.sort(reverse = True, cmp = \
				lambda x, y: cmp(x.sortidx, y.sortidx))

		if full:
			ws = [ w.to_dict(self.basepath) for w in ws ]
		else:
			ws = [ w.id for w in ws ]

		return ws

	def delete_wbos(self, ids, parent_id, older, newer, limit, offset,
			sort):
		wids = self.list_wbos(ids, parent_id = parent_id,
				older = older, newer = newer, limit = limit,
				offset = offset, sort = sort)
		for wid in wids:
			w = self.wbos[wid]
			ppath = self.basepath + '/' + payload_path(wid)
			if os.path.exists(ppath):
				os.unlink(ppath)
			del self.wbos[wid]
		self.save_wbos()


#
# HTTP request handling
#

# TODO: handle  X-If-Unmodified-Since header

class InvalidPathError (Exception):
	pass

def path_info(path):
	"""Checks the basic path information, and returns the username and the
	path components (as a list). Raises an exception if the path is
	invalid."""
	p = path.strip('/').split('/')
	if p[0] == 'user':
		p.pop(0)

	if len(p) < 3:
		raise InvalidPathError

	if p[0] not in ('0.5', '1.0', '1.1'):
		raise InvalidPathError

	if not os.path.exists(data_path + '/' + fsencode(p[1])):
		raise InvalidPathError

	return p[1], p[2:]

def debug(msg, *args):
	return
	m = msg
	if args:
		a = [ str(arg) for arg in args ]
		m += ' ' + ' '.join(a)
	sys.stderr.write('DEBUG: ' + m + '\n')

def error(http, msg = None):
	et = {
		400: 'Bad Request',
		401: 'Unauthorized',
		404: 'Not Found',
		500: 'Internal server error',
		503: 'Service Unavailable',
	}

	debug('pyweave err %d: %s\n' % (http, msg))
	print 'Status: ' + str(http) + ' ' + et.get(http, str(http))
	if msg:
		print 'X-Weave-Alert:', msg
		print 'Content-type: text/plain'
		print
		print msg
	print

def bad_request(msg = None):
	error(400, msg)

def output_plain(obj):
	print 'X-Weave-Timestamp: %.2f' % time.time()
	print "Content-type: text/plain"
	print
	print obj

def output(obj, timestamp = None):
	if timestamp is None:
		timestamp = time.time()
	print 'X-Weave-Timestamp: %.2f' % timestamp

	accept = os.environ.get('HTTP_ACCEPT', 'application/json')
	if accept == 'application/whoisi':
		import struct
		print "Content-type: application/whoisi"
		print
		sys.stdout.write(struct.pack('!I', obj.id) + json.dumps(obj))
	elif accept == 'application/newlines':
		print "Content-type: application/newlines"
		print
		# We assume we have multiple objects in a list
		for o in obj:
			print json.dumps(o).replace('\n', r'\u000a')
	else:
		print "Content-type: application/json"
		print
		print json.dumps(obj)

def fromform(form, name, convert = None):
	# if no form is sent, form.getfirst() raises an exception, so handle
	# this by avoiding the call
	if form:
		v = form.getfirst(name, None)
	else:
		v = None
	if convert and v is not None:
		return convert(v)
	return v

def read_stdin():
	s = sys.stdin.read(int(os.environ['CONTENT_LENGTH']))
	debug('STDIN: ' + repr(s))
	return s

def do_get(path, storage):
	if len(path) < 2:
		bad_request("Path too short")
		return

	if path[0] == 'info':
		if path[1] == 'collections':
			cs = storage.list_collections()
			d = {}
			for c in cs:
				d[c.id] = c.mtime
			output(d)
		elif path[1] == 'collection_counts':
			cs = storage.list_collections()
			d = {}
			for c in cs:
				d[c.id] = len(c.wbos)
			output(d)
		elif path[1] == 'quota':
			# TODO
			output((1, 100 * 1024))
		else:
			bad_request("Unknown info request")
	elif path[0] == 'storage':
		try:
			c = storage.get_collection(path[1])
		except KeyError:
			# dummy empty collection
			c = Collection(storage.basepath, path[1])

		if len(path) == 3:
			if path[2] in c.wbos:
				w = c.wbos[path[2]]
				output(w.to_dict(c.basepath))
			else:
				error(404, "WBO not found")
		else:
			form = cgi.FieldStorage()
			ids = fromform(form, "ids")
			predecessorid = fromform(form, "predecessorid")
			parentid = fromform(form, "parentid")
			older = fromform(form, "older", float)
			newer = fromform(form, "newer", float)
			full = fromform(form, "full")
			index_above = fromform(form, "index_above", int)
			index_below = fromform(form, "index_below", int)
			limit = fromform(form, "limit", int)
			offset = fromform(form, "offset", int)
			sort = fromform(form, "sort")

			wl = c.list_wbos(ids, predecessorid, parentid, older,
					newer, full, index_above, index_below,
					limit, offset, sort)
			output(wl)
	elif path[0] == 'node' and path[1] == 'weave':
		try:
			if os.environ['HTTPS'] == 'on':
				output_plain('https://' + os.environ['SERVER_NAME'] + server_path)
			else:
				output_plain('http://' + os.environ['SERVER_NAME'] + server_path)
		except KeyError:
			output_plain('http://' + os.environ['SERVER_NAME'] + server_path)
				
	else:
		bad_request("Unknown GET request")

def do_put(path, storage):
	if path[0] != 'storage' or len(path) != 3:
		bad_request("Malformed PUT path")
		return

	c = storage.get_collection(path[1], create = True)
	ts = c.put_wbo_json(path[2], json.loads(read_stdin()))
	c.save_wbos()
	output(ts, timestamp = ts)

def do_post(path, storage):
	if path[0] != 'storage' or len(path) != 2:
		bad_request("Malformed POST path")
		return

	ts = time.time()
	c = storage.get_collection(path[1], create = True)
	objs = json.loads(read_stdin())

	res = {
		'modified': ts,
		'success': [],
		'failed': {},
	}
	for o in objs:
		c.put_wbo_json(o['id'], o, ts)
		res['success'].append(o['id'])
	c.save_wbos()
	output(res, timestamp = ts)

def do_delete(path, storage):
	if path[0] != 'storage':
		bad_request("Malformed DELETE path")
		return

	try:
		c = storage.get_collection(path[1])
	except KeyError:
		output(time.time())
		return

	if len(path) == 3:
		del c.wbos[path[2]]
		c.save_wbos()
		output(time.time())
	else:
		form = cgi.FieldStorage()
		ids = fromform(form, "ids")
		parentid = fromform(form, "parentid")
		older = fromform(form, "older", float)
		newer = fromform(form, "newer", float)
		limit = fromform(form, "limit", int)
		offset = fromform(form, "offset", int)
		sort = fromform(form, "sort")

		wl = c.delete_wbos(ids, parentid, older, newer, limit,
				offset, sort)
		output(time.time())

def user_lock(user):
	"""Locks the given user, returns a lock token to pass to
	user_unlock()."""
	fd = open(data_path + '/' + user + '/lock', 'w')
	fcntl.lockf(fd, fcntl.LOCK_EX)
	return fd

def user_unlock(token):
	"""Unlocks the given user. The token must be the one returned by
	user_lock()."""
	# Note we don't really need to do this, as the garbage collector
	# should take care of disposing the fd as it's no longer needed, and
	# the lock goes away on close(). We just do it to be tidy.
	fcntl.lockf(token, fcntl.LOCK_UN)
	token.close()

def handle_cgi():
	user = os.environ.get('REMOTE_USER', None)
	method = os.environ['REQUEST_METHOD']
	try:
		path_user, path = path_info(os.environ['PATH_INFO'])
	except InvalidPathError:
		error(404, "Path error")
		return

	if user != path_user:
		error(401, "User/path mismatch: %s - %s" % (user, path_user))
		return

	lock_token = user_lock(user)

	storage = Storage(data_path + '/' + user)

	debug("HANDLE", user, storage, method)
	if method == 'GET':
		do_get(path, storage)
	elif method == 'PUT':
		do_put(path, storage)
	elif method == 'POST':
		do_post(path, storage)
	elif method == 'DELETE':
		do_delete(path, storage)

	user_unlock(lock_token)

def handle_cmd():
	print "This is a CGI application."
	print "It only runs inside a web server."
	return 1

if __name__ == '__main__':
	if os.environ.has_key('GATEWAY_INTERFACE'):
		try:
			handle_cgi()
		except Exception, e:
			error(500, "Unhandled exception")
			raise
	else:
		sys.exit(handle_cmd())

