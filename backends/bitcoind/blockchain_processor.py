import ast
import hashlib
from json import dumps, loads
#import leveldb
import plyvel
import os
from Queue import Queue
import random
import sys
import time
import threading
import traceback
import urllib

from backends.bitcoind import deserialize
from processor import Processor, print_log
from utils import *

from storage import Storage

class BlockchainProcessor(Processor):

    def __init__(self, config, shared):
        Processor.__init__(self)


        self.mtimes = {} # monitoring
        self.shared = shared
        self.config = config
        self.up_to_date = False
        self.watched_addresses = []
        self.history_cache = {}
        self.chunk_cache = {}
        self.cache_lock = threading.Lock()
        self.headers_data = ''
        self.headers_path = config.get('leveldb', 'path_plyvel')
        try:
            self.test_reorgs = config.get('leveldb', 'test_reorgs')   # simulate random blockchain reorgs
        except:
            self.test_reorgs = False

        self.mempool_addresses = {}
        self.mempool_hist = {}
        self.mempool_hashes = []
        self.mempool_lock = threading.Lock()

        self.address_queue = Queue()
        self.storage = Storage(config, shared, self.test_reorgs)

        self.dblock = threading.Lock()

        self.bitcoind_url = 'http://%s:%s@%s:%s/' % (
            config.get('bitcoind', 'user'),
            config.get('bitcoind', 'password'),
            config.get('bitcoind', 'host'),
            config.get('bitcoind', 'port'))

        while True:
            try:
                self.bitcoind('getinfo')
                break
            except:
                print_log('cannot contact bitcoind...')
                time.sleep(5)
                continue

        #self.height = 0
        self.sent_height = 0
        self.sent_header = None


        # catch_up headers
        self.init_headers(self.storage.height)

        #self.storage.update_all_hashes()

        threading.Timer(0, lambda: self.catch_up(sync=False)).start()
        while not shared.stopped() and not self.up_to_date:
            try:
                time.sleep(1)
            except:
                print "keyboard interrupt: stopping threads"
                shared.stop()

        if self.shared.stopped():
            sys.exit(0)

        print_log("Blockchain is up to date.")
        self.memorypool_update()
        print_log("Memory pool initialized.")


        threading.Timer(10, self.main_iteration).start()

    def bitcoind(self, method, params=[]):
        postdata = dumps({"method": method, 'params': params, 'id': 'jsonrpc'})
        try:
            respdata = urllib.urlopen(self.bitcoind_url, postdata).read()
        except:
            traceback.print_exc(file=sys.stdout)
            self.shared.stop()

        r = loads(respdata)
        if r['error'] is not None:
            raise BaseException(r['error'])
        return r.get('result')


    def block2header(self, b):
        return {
            "block_height": b.get('height'),
            "version": b.get('version'),
            "prev_block_hash": b.get('previousblockhash'),
            "merkle_root": b.get('merkleroot'),
            "timestamp": b.get('time'),
            "bits": int(b.get('bits'), 16),
            "nonce": b.get('nonce'),
        }

    def get_header(self, height):
        block_hash = self.bitcoind('getblockhash', [height])
        b = self.bitcoind('getblock', [block_hash])
        return self.block2header(b)

    def init_headers(self, db_height):
        self.chunk_cache = {}
        self.headers_filename = os.path.join(self.headers_path, 'blockchain_headers')

        if os.path.exists(self.headers_filename):
            height = os.path.getsize(self.headers_filename)/80 - 1   # the current height
            if height > 0:
                prev_hash = self.hash_header(self.read_header(height))
            else:
                prev_hash = None
        else:
            open(self.headers_filename, 'wb').close()
            prev_hash = None
            height = -1

        if height < db_height:
            print_log("catching up missing headers:", height, db_height)

        try:
            while height < db_height:
                height = height + 1
                header = self.get_header(height)
                if height > 1:
                    assert prev_hash == header.get('prev_block_hash')
                self.write_header(header, sync=False)
                prev_hash = self.hash_header(header)
                if (height % 1000) == 0:
                    print_log("headers file:", height)
        except KeyboardInterrupt:
            self.flush_headers()
            sys.exit()

        self.flush_headers()

    def hash_header(self, header):
        return rev_hex(Hash(header_to_string(header).decode('hex')).encode('hex'))

    def read_header(self, block_height):
        if os.path.exists(self.headers_filename):
            with open(self.headers_filename, 'rb') as f:
                f.seek(block_height * 80)
                h = f.read(80)
            if len(h) == 80:
                h = header_from_string(h)
                return h

    def read_chunk(self, index):
        with open(self.headers_filename, 'rb') as f:
            f.seek(index*2016*80)
            chunk = f.read(2016*80)
        return chunk.encode('hex')

    def write_header(self, header, sync=True):
        if not self.headers_data:
            self.headers_offset = header.get('block_height')

        self.headers_data += header_to_string(header).decode('hex')
        if sync or len(self.headers_data) > 40*100:
            self.flush_headers()

        with self.cache_lock:
            chunk_index = header.get('block_height')/2016
            if self.chunk_cache.get(chunk_index):
                self.chunk_cache.pop(chunk_index)

    def pop_header(self):
        # we need to do this only if we have not flushed
        if self.headers_data:
            self.headers_data = self.headers_data[:-40]

    def flush_headers(self):
        if not self.headers_data:
            return
        with open(self.headers_filename, 'rb+') as f:
            f.seek(self.headers_offset*80)
            f.write(self.headers_data)
        self.headers_data = ''

    def get_chunk(self, i):
        # store them on disk; store the current chunk in memory
        with self.cache_lock:
            chunk = self.chunk_cache.get(i)
            if not chunk:
                chunk = self.read_chunk(i)
                self.chunk_cache[i] = chunk

        return chunk

    def get_mempool_transaction(self, txid):
        try:
            raw_tx = self.bitcoind('getrawtransaction', [txid, 0, -1])
        except:
            return None

        vds = deserialize.BCDataStream()
        vds.write(raw_tx.decode('hex'))

        return deserialize.parse_Transaction(vds, is_coinbase=False)

    def get_history(self, addr, cache_only=False):
        with self.cache_lock:
            hist = self.history_cache.get(addr)
        if hist is not None:
            return hist
        if cache_only:
            return -1

        with self.dblock:
            try:
                h = self.storage.get_history(str((addr)))
                hist = self.storage.deserialize(h)
            except:
                self.shared.stop()
                raise
            if hist:
                is_known = True
            else:
                hist = []
                is_known = False

        # sort history, because redeeming transactions are next to the corresponding txout
        hist.sort(key=lambda tup: tup[2])

        # add memory pool
        with self.mempool_lock:
            for txid in self.mempool_hist.get(addr, []):
                hist.append((txid, 0, 0))

        # uniqueness
        hist = set(map(lambda x: (x[0], x[2]), hist))

        # convert to dict
        hist = map(lambda x: {'tx_hash': x[0], 'height': x[1]}, hist)

        # add something to distinguish between unused and empty addresses
        if hist == [] and is_known:
            hist = ['*']

        with self.cache_lock:
            self.history_cache[addr] = hist
        return hist

    def get_status(self, addr, cache_only=False):
        tx_points = self.get_history(addr, cache_only)
        if cache_only and tx_points == -1:
            return -1

        if not tx_points:
            return None
        if tx_points == ['*']:
            return '*'
        status = ''
        for tx in tx_points:
            status += tx.get('tx_hash') + ':%d:' % tx.get('height')
        return hashlib.sha256(status).digest().encode('hex')

    def get_merkle(self, tx_hash, height):

        block_hash = self.bitcoind('getblockhash', [height])
        b = self.bitcoind('getblock', [block_hash])
        tx_list = b.get('tx')
        tx_pos = tx_list.index(tx_hash)

        merkle = map(hash_decode, tx_list)
        target_hash = hash_decode(tx_hash)
        s = []
        while len(merkle) != 1:
            if len(merkle) % 2:
                merkle.append(merkle[-1])
            n = []
            while merkle:
                new_hash = Hash(merkle[0] + merkle[1])
                if merkle[0] == target_hash:
                    s.append(hash_encode(merkle[1]))
                    target_hash = new_hash
                elif merkle[1] == target_hash:
                    s.append(hash_encode(merkle[0]))
                    target_hash = new_hash
                n.append(new_hash)
                merkle = merkle[2:]
            merkle = n

        return {"block_height": height, "merkle": s, "pos": tx_pos}



    def deserialize_block(self, block):
        txlist = block.get('tx')
        tx_hashes = []  # ordered txids
        txdict = {}     # deserialized tx
        is_coinbase = True
        for raw_tx in txlist:
            tx_hash = hash_encode(Hash(raw_tx.decode('hex')))
            tx_hashes.append(tx_hash)
            vds = deserialize.BCDataStream()
            vds.write(raw_tx.decode('hex'))
            tx = deserialize.parse_Transaction(vds, is_coinbase)
            txdict[tx_hash] = tx
            is_coinbase = False
        return tx_hashes, txdict

    def mtime(self, name):
        now = time.clock()
        if name != '':
            delta = now - self.now
            t = self.mtimes.get(name, 0)
            self.mtimes[name] = t + delta
        self.now = now

    def print_mtime(self):
        s = ''
        for k, v in self.mtimes.items():
            s += k+':'+"%.2f"%v+' '
        print_log(s)


    def import_block(self, block, block_hash, block_height, sync, revert=False):
        self.mtime('')

        touched_addr = []

        # deserialize transactions
        tx_hashes, txdict = self.deserialize_block(block)

        # undo info
        if revert:
            undo_info = self.storage.get_undo_info(block_height)
        else:
            undo_info = {}

        # process
        if revert:
            tx_hashes = tx_hashes[::-1]

        for txid in tx_hashes:  # must be ordered
            tx = txdict[txid]
            if not revert:
                undo = self.storage.import_transaction(txid, tx, block_height, touched_addr)
                undo_info[txid] = undo
            else:
                undo = undo_info.pop(txid)
                self.storage.revert_transaction(txid, tx, block_height, touched_addr, undo)

        if revert:
            assert undo_info == {}

        self.mtime('process')
            
        # add undo info
        if not revert:
            self.storage.write_undo_info(block_height, self.bitcoind_height, undo_info)

        # add the max
        self.storage.db.put('height', repr( (block_hash, block_height, self.storage.db_version) ))

        for addr in touched_addr:
            self.invalidate_cache(addr)



    def add_request(self, request):
        # see if we can get if from cache. if not, add to queue
        if self.process(request, cache_only=True) == -1:
            self.queue.put(request)

    def process(self, request, cache_only=False):

        message_id = request['id']
        method = request['method']
        params = request.get('params', [])
        result = None
        error = None

        if method == 'blockchain.numblocks.subscribe':
            result = self.storage.height

        elif method == 'blockchain.headers.subscribe':
            result = self.header

        elif method == 'blockchain.address.subscribe':
            try:
                address = params[0]
                result = self.get_status(address, cache_only)
                self.watch_address(address)
            except BaseException, e:
                error = str(e) + ': ' + address
                print_log("error:", error)

        elif method == 'blockchain.address.unsubscribe':
            try:
                password = params[0]
                address = params[1]
                if password == self.config.get('server', 'password'):
                    self.watched_addresses.remove(address)
                    # print_log('unsubscribed', address)
                    result = "ok"
                else:
                    print_log('incorrect password')
                    result = "authentication error"
            except BaseException, e:
                error = str(e) + ': ' + address
                print_log("error:", error)

        elif method == 'blockchain.address.get_history':
            try:
                address = params[0]
                result = self.get_history(address, cache_only)
            except BaseException, e:
                error = str(e) + ': ' + address
                print_log("error:", error)

        elif method == 'blockchain.block.get_header':
            if cache_only:
                result = -1
            else:
                try:
                    height = params[0]
                    result = self.get_header(height)
                except BaseException, e:
                    error = str(e) + ': %d' % height
                    print_log("error:", error)

        elif method == 'blockchain.block.get_chunk':
            if cache_only:
                result = -1
            else:
                try:
                    index = params[0]
                    result = self.get_chunk(index)
                except BaseException, e:
                    error = str(e) + ': %d' % index
                    print_log("error:", error)

        elif method == 'blockchain.transaction.broadcast':
            try:
                txo = self.bitcoind('sendrawtransaction', params)
                print_log("sent tx:", txo)
                result = txo
            except BaseException, e:
                result = str(e)  # do not send an error
                print_log("error:", result, params)

        elif method == 'blockchain.transaction.get_merkle':
            if cache_only:
                result = -1
            else:
                try:
                    tx_hash = params[0]
                    tx_height = params[1]
                    result = self.get_merkle(tx_hash, tx_height)
                except BaseException, e:
                    error = str(e) + ': ' + repr(params)
                    print_log("get_merkle error:", error)

        elif method == 'blockchain.transaction.get':
            try:
                tx_hash = params[0]
                height = params[1]
                result = self.bitcoind('getrawtransaction', [tx_hash, 0, height])
            except BaseException, e:
                error = str(e) + ': ' + repr(params)
                print_log("tx get error:", error)

        else:
            error = "unknown method:%s" % method

        if cache_only and result == -1:
            return -1

        if error:
            self.push_response({'id': message_id, 'error': error})
        elif result != '':
            self.push_response({'id': message_id, 'result': result})

    def watch_address(self, addr):
        if addr not in self.watched_addresses:
            self.watched_addresses.append(addr)

    def catch_up(self, sync=True):
        t1 = time.time()

        while not self.shared.stopped():
            # are we done yet?
            info = self.bitcoind('getinfo')
            self.bitcoind_height = info.get('blocks')
            bitcoind_block_hash = self.bitcoind('getblockhash', [self.bitcoind_height])
            if self.storage.last_hash == bitcoind_block_hash:
                self.up_to_date = True
                break

            # not done..
            self.up_to_date = False
            next_block_hash = self.bitcoind('getblockhash', [self.storage.height + 1])
            next_block = self.bitcoind('getblock', [next_block_hash, 1])

            # fixme: this is unsafe, if we revert when the undo info is not yet written
            revert = (random.randint(1, 100) == 1) if self.test_reorgs else False

            if (next_block.get('previousblockhash') == self.storage.last_hash) and not revert:

                self.import_block(next_block, next_block_hash, self.storage.height+1, sync)
                self.storage.height = self.storage.height + 1
                self.write_header(self.block2header(next_block), sync)
                self.storage.last_hash = next_block_hash

                if self.storage.height % 10 == 0 and not sync:
                    t2 = time.time()
                    print_log("catch_up: block %d (%.3fs)" % (self.storage.height, t2 - t1), self.storage.get_root_hash().encode('hex'))
                    # self.print_mtime()
                    t1 = t2

                #if self.storage.height == 1000:
                #    self.shared.stop()

            else:
                # revert current block
                block = self.bitcoind('getblock', [self.storage.last_hash, 1])
                print_log("blockchain reorg", self.storage.height, block.get('previousblockhash'), self.storage.last_hash)
                self.import_block(block, self.storage.last_hash, self.storage.height, sync, revert=True)
                self.pop_header()
                self.flush_headers()

                self.storage.height -= 1

                # read previous header from disk
                self.header = self.read_header(self.storage.height)
                self.storage.last_hash = self.hash_header(self.header)

            #print "z", self.storage.height, Hash(self.storage.root_hash).encode('hex'), self.storage.root_hash

        self.header = self.block2header(self.bitcoind('getblock', [self.storage.last_hash]))

        if self.shared.stopped(): 
            print_log( "closing database" )
            #self.storage.print_all()
            self.storage.close()

    def memorypool_update(self):
        mempool_hashes = self.bitcoind('getrawmempool')

        touched_addresses = []
        for tx_hash in mempool_hashes:
            if tx_hash in self.mempool_hashes:
                continue

            tx = self.get_mempool_transaction(tx_hash)
            if not tx:
                continue

            mpa = self.mempool_addresses.get(tx_hash, [])
            for x in tx.get('inputs'):
                # we assume that the input address can be parsed by deserialize(); this is true for Electrum transactions
                addr = x.get('address')
                if addr and addr not in mpa:
                    mpa.append(addr)
                    touched_addresses.append(addr)

            for x in tx.get('outputs'):
                addr = x.get('address')
                if addr and addr not in mpa:
                    mpa.append(addr)
                    touched_addresses.append(addr)

            self.mempool_addresses[tx_hash] = mpa
            self.mempool_hashes.append(tx_hash)

        # remove older entries from mempool_hashes
        self.mempool_hashes = mempool_hashes

        # remove deprecated entries from mempool_addresses
        for tx_hash, addresses in self.mempool_addresses.items():
            if tx_hash not in self.mempool_hashes:
                self.mempool_addresses.pop(tx_hash)
                for addr in addresses:
                    touched_addresses.append(addr)

        # rebuild mempool histories
        new_mempool_hist = {}
        for tx_hash, addresses in self.mempool_addresses.items():
            for addr in addresses:
                h = new_mempool_hist.get(addr, [])
                if tx_hash not in h:
                    h.append(tx_hash)
                new_mempool_hist[addr] = h

        with self.mempool_lock:
            self.mempool_hist = new_mempool_hist

        # invalidate cache for touched addresses
        for addr in touched_addresses:
            self.invalidate_cache(addr)


    def invalidate_cache(self, address):
        with self.cache_lock:
            if address in self.history_cache:
                print_log("cache: invalidating", address)
                self.history_cache.pop(address)

        if address in self.watched_addresses:
            # TODO: update cache here. if new value equals cached value, do not send notification
            self.address_queue.put(address)

    def main_iteration(self):
        if self.shared.stopped():
            print_log("blockchain processor terminating")
            print_log("closing database")
            self.storage.close()
            return

        with self.dblock:
            t1 = time.time()
            self.catch_up()
            t2 = time.time()

        self.memorypool_update()

        if self.sent_height != self.storage.height:
            self.sent_height = self.storage.height
            self.push_response({
                'id': None,
                'method': 'blockchain.numblocks.subscribe',
                'params': [self.storage.height],
            })

        if self.sent_header != self.header:
            print_log("blockchain: %d (%.3fs)" % (self.storage.height, t2 - t1))
            self.sent_header = self.header
            self.push_response({
                'id': None,
                'method': 'blockchain.headers.subscribe',
                'params': [self.header],
            })

        while True:
            try:
                addr = self.address_queue.get(False)
            except:
                break
            if addr in self.watched_addresses:
                status = self.get_status(addr)
                self.push_response({
                    'id': None,
                    'method': 'blockchain.address.subscribe',
                    'params': [addr, status],
                })

        if not self.shared.stopped():
            threading.Timer(10, self.main_iteration).start()
        else:
            print_log("blockchain processor terminating")
