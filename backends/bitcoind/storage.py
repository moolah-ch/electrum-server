import plyvel, ast, hashlib, traceback
from processor import print_log
from utils import *


"""
todo: 
 * store spent histories 
 * miners
"""

DEBUG = False


class Storage(object):

    def __init__(self, config, shared, test_reorgs):
        # address: 20 bytes + 1 (we don't need to add that byte)

        self.dbpath = config.get('leveldb', 'path_plyvel')
        self.pruning_limit = config.getint('leveldb', 'pruning_limit')
        self.shared = shared
        self.test_reorgs = test_reorgs
        try:
            self.db = plyvel.DB(self.dbpath, create_if_missing=True, paranoid_checks=True, compression=None)
        except:
            traceback.print_exc(file=sys.stdout)
            self.shared.stop()

        self.db_version = 2 # increase this when database needs to be updated
        try:
            self.last_hash, self.height, db_version = ast.literal_eval(self.db.get('height'))
            print_log("Database version", self.db_version)
            print_log("Blockchain height", self.height)
        except:
            #traceback.print_exc(file=sys.stdout)
            print_log('initializing database')
            self.height = 0
            self.last_hash = '000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f'
            db_version = self.db_version

            self.init_batch()
            self.put_node('a', '', 0, None)
            self.write_batch()

        # check version
        if self.db_version != db_version:
            print_log("Your database '%s' is deprecated. Please create a new database"%self.dbpath)
            self.shared.stop()
            return




    # convert between bitcoin addresses and 21 bytes keys used for storage. 
    def address_to_key(self, addr):
        return 'a' + bc_address_to_hash_160(addr)

    def key_to_address(self, addr):
        return hash_160_to_bc_address(addr[1:])


    def db_get(self, key):
        try:
            return self.db.get(key)
        except:
            print_log("db get error", key)
            traceback.print_exc(file=sys.stdout)
            self.shared.stop()
            raise

    def get_history(self, addr):
        addr = self.address_to_key(addr)

        x = self.get_node(addr)
        if x is None: 
            return ''
        try:
            _hash, v, h = x
            return h
        except:
            traceback.print_exc(file=sys.stdout)
            self.shared.stop()
            raise


    def get_node(self, key):
        s = self.batch_keys.get(key)
        if s is None:
            s = self.db_get(key)
            if s is None: 
                return 
        return self.parse_node(s)


    def get_address(self, txi):
        txi = 'b' + txi
        addr = self.batch_keys.get(txi)
        if not addr: 
            addr = self.db_get(txi)
            self.batch_keys[txi] = addr

        return self.key_to_address(addr) if addr else None



    def put(self, key, value):
        self.batch_keys[key] = value

    def delete(self, key):
        self.delete_list.append(key)


    def get_undo_info(self, height):
        s = self.db.get("undo%d" % (height % 100))
        if s is None: print_log("no undo info for ", height)
        return eval(s)


    def write_undo_info(self, height, bitcoind_height, undo_info):
        if height > bitcoind_height - 100 or self.test_reorgs:
            self.batch_keys["undo%d" % (height % 100)] =  repr(undo_info)


    def read_addresses(self, addr_list):
        #addr_list = map( self.address_to_key, addr_list)
        #addr_list.sort()
        for addr in addr_list:
            k = self.address_to_key(addr)
            self.batch_addr[k] = self.get_history(addr)



    def init_batch(self):
        self.batch_addr = {}  # addresses for the tree
        self.batch_keys = {}  # everything else
        self.delete_list = []


    def write_batch(self):
        batch = self.db.write_batch()
        for k, v in self.batch_keys.items():
            batch.put(k, v)
        for key in self.delete_list:
            batch.delete(key)
        batch.write()
        self.init_batch()


    def write_addresses(self):
        path_list = []

        # add new addresses to the tree
        for addr, serialized_hist in self.batch_addr.items():
            if serialized_hist:
                path = self.add_address(addr, serialized_hist)
                path_list.append(path)
            else:
                self.delete_address(addr)

        new_addresses = map(self.key_to_address, self.batch_addr.keys())

        self.write_batch()

        # update node hashes
        for path in path_list:
            for x in path[::-1]:
                self.update_node_hash(x)

        self.write_batch()

        # return list of addresses, in order to invalidate cache
        return new_addresses 


    def common_prefix(self, word1, word2):
        max_len = min(len(word1),len(word2))
        for i in range(max_len):
            if word2[i] != word1[i]:
                index = i
                break
        else:
            index = max_len
        return word1[0:index]

    def get_children(self, x):
        i = self.db.iterator()
        l = 0
        while l <256:
            i.seek(x+chr(l))
            k, v = i.next()
            if k.startswith(x+chr(l)): 
                yield k, v
                l += 1
            elif k.startswith(x): 
                yield k, v
                l = ord(k[len(x)]) + 1
            else: 
                break

    def put_node(self, key, _hash, value, item):
        self.batch_keys[key] = repr( (_hash, value, item) )


    def parse_node(self, s):
        _hash, v, h = ast.literal_eval( s )
        return _hash, v, h


    def add_address(self, target, serialized_hist):
        assert target[0] == 'a'

        word = target[1:]
        key = 'a'
        path = [ 'a' ]
        i = self.db.iterator()

        while key != target:

            i.seek(key + word[0])
            try:
                new_key, _ = i.next()
                is_child = new_key.startswith(key + word[0])
            except StopIteration:
                is_child = False

            if is_child:
  
                if target.startswith(new_key):
                    # add value to the child node
                    key = new_key
                    word = target[len(key):]
                    if key == target:
                        break
                    else:
                        assert key not in path
                        path.append(key)
                else:
                    # prune current node and add new node
                    prefix = self.common_prefix(new_key, target)
                    self.put_node(prefix, None, 0, None)
                    path.append(prefix)
                    break

            else:
                assert key in path
                break


        # compute hash and value of the final node
        utxo = self.get_unspent(serialized_hist)
        _hash = self.hash_tree(map( lambda x:x[4], utxo)) if utxo else None
        value = sum( map( lambda x:x[2], utxo ) )

        # write 
        self.put_node(target,  _hash, value, serialized_hist)
        return path




    def get_path(self, target):
        assert target[0] == 'a'

        word = target[1:]
        key = 'a'
        path = [ 'a' ]
        i = self.db.iterator()

        while key != target:
            
            i.seek(key + word[0])
            try:
                new_key, _ = i.next()
                is_child = new_key.startswith(key + word[0])
            except StopIteration:
                is_child = False

            if is_child:
  
                if target.startswith(new_key):
                    # add value to the child node
                    key = new_key
                    word = target[len(key):]
                    if key == target:
                        break
                    else:
                        assert key not in path
                        path.append(key)
                else:
                    #print 'new key', (key+word[0]).encode('hex'), new_key.encode('hex')
                    return False

            else:
                assert key in path
                break

        return path


    def delete_address(self, addr):
        path = self.get_path(addr)
        if path is False:
            # print_log("address not in tree", addr.encode('hex'))
            return

        #print "removing", addr.encode('hex')
        self.db.delete(addr)  # if I use delete_list I need to make sure get_children still works

        for i in path[::-1]:
            #remove key if it has a single child
            ch = [x for x in self.get_children(i)]
            if len(ch) == 1:
                #print "removing parent", i.encode('hex')
                self.db.delete(i)
            else:
                #print "keeping", repr(i), ch
                pass






    def hash_tree(self, base_list):
        # etotheipi_ : (TxHash:TxOutIndex:Value) would be hashed to produce the node IDs
        if DEBUG: return "h"

        merkle = base_list
        s = []
        while len(merkle) != 1:
            if len(merkle) % 2:
                merkle.append(merkle[-1])
            n = []
            while merkle:
                new_hash = Hash(merkle[0] + merkle[1])
                n.append(new_hash)
                merkle = merkle[2:]
            merkle = n
        return merkle[0]


    def get_parent(self, x):
        """ return parent and skip string"""
        i = self.db.iterator()
        for j in range(len(x)):
            p = x[0:-j-1]
            i.seek(p)
            k, v = i.next()
            if x.startswith(k) and x!=k: 
                break
        else: raise
        return k, x[len(k)+1:]



    def update_node_hash(self, x):

        # get the hashes of children
        hashes = []
        _, value, history = self.get_node(x)

        values = []
        for k, v in self.get_children(x):
            
            _hash, v, _ = ast.literal_eval(v)
            # _hash is None for fully spent addresses 
            if _hash is not None:
                hashes.append(_hash)
                values.append(v)

        value = sum( values )

        # final hash
        if x != 'a':
            parent, skip_string = self.get_parent(x)
        else:
            skip_string = ''

        _hash = self.hash( skip_string + ''.join(hashes) ) if hashes else None
        self.put_node(x, _hash, value, history)
        
    def hash(self, x):
        if DEBUG: return "hash("+x+")"
        return Hash(x)

    def get_root_hash(self):
        item = self.db.get('a')
        _h, v, c = ast.literal_eval(item)
        return _h

    def print_all(self):
        i = self.db.iterator()
        for k,v in i:
            if k and k[0] == 'a':
                addr = k[1:].encode('hex')
                _h, v, c = ast.literal_eval(v)
                if DEBUG: 
                    print addr, "->", _h if _h else None, v
                else:
                    print addr, "->", _h.encode('hex') if _h else None, v
        print " ---------- "

    def close(self):
        self.db.close()


    def add_to_history(self, addr, tx_hash, tx_pos, value, tx_height):
        addr = self.address_to_key(addr)

        "add tx output to the history of addr"
        "also creates a backlink: txo-> addr"

        # keep it sorted
        s = self.serialize_item(tx_hash, tx_pos, value, tx_height)
        assert len(s) == 48

        serialized_hist = self.batch_addr[addr]

        l = len(serialized_hist)/48
        for i in range(l-1, -1, -1):
            item = serialized_hist[48*i:48*(i+1)]
            item_height = int(rev_hex(item[44:47].encode('hex')), 16)
            if item_height <= tx_height:
                serialized_hist = serialized_hist[0:48*(i+1)] + s + serialized_hist[48*(i+1):]
                break
        else:
            serialized_hist = s + serialized_hist

        self.batch_addr[addr] = serialized_hist

        # backlink
        txo = (tx_hash + int_to_hex(tx_pos, 4)).decode('hex')
        self.batch_keys['b'+txo] = addr




    def revert_add_to_history(self, addr, tx_hash, tx_pos, value, tx_height):
        addr = self.address_to_key(addr)

        serialized_hist = self.batch_addr[addr]
        s = self.serialize_item(tx_hash, tx_pos, value, tx_height)
        if serialized_hist.find(s) == -1: raise
        serialized_hist = serialized_hist.replace(s, '')
        self.batch_addr[addr] = serialized_hist



    def prune_history(self, addr, undo):
        raise
        addr = self.address_to_key(addr)

        # remove items that have bit set to one
        if undo.get(addr) is None: undo[addr] = []

        serialized_hist = self.batch_addr[addr]
        l = len(serialized_hist)/96
        for i in range(l):
            if len(serialized_hist)/96 < self.pruning_limit: break
            item = serialized_hist[96*i:96*(i+1)] 
            if item[47:48] == chr(1):
                assert item[95:96] == chr(2)
                serialized_hist = serialized_hist[0:96*i] + serialized_hist[96*(i+1):]
                undo[addr].append(item)  # items are ordered
        self.batch_addr[addr] = serialized_hist


    def revert_set_spent(self, addr, undo):
        addr = self.address_to_key(addr)

        # restore removed items
        serialized_hist = self.batch_addr[addr]

        if undo.get(addr) is not None: 
            itemlist = undo.pop(addr)
        else:
            return 

        if not itemlist: return

        l = len(serialized_hist)/48
        tx_item = ''
        for i in range(l-1, -1, -1):
            if tx_item == '':
                if not itemlist: 
                    break
                else:
                    tx_item = itemlist.pop(-1) # get the last element
                    tx_height = int(rev_hex(tx_item[36:39].encode('hex')), 16)
            
            item = serialized_hist[48*i:48*(i+1)]
            item_height = int(rev_hex(item[44:47].encode('hex')), 16)

            if item_height < tx_height:
                serialized_hist = serialized_hist[0:48*(i+1)] + tx_item + serialized_hist[48*(i+1):]
                tx_item = ''

        else:
            serialized_hist = ''.join(itemlist) + tx_item + serialized_hist

        self.batch_addr[addr] = serialized_hist


    def set_spent(self, addr, txi, txid, index, height, undo):
        addr = self.address_to_key(addr)
        if undo.get(addr) is None: undo[addr] = []

        # spent_key = 'h'+ addr[1:]

        utx_hist = self.batch_addr[addr]
        # spent_hist = self.batch_keys[spent_key]

        l = len(utx_hist)/48
        for i in range(l):
            item = utx_hist[48*i:48*(i+1)]
            if item[0:36] == txi:
                utx_hist = utx_hist[0:48*i] + utx_hist[48*(i+1):]
                undo[addr].append(item)
                # new_item = item[0:47] + chr(1) + self.serialize_item(txid, index, 0, height, chr(2))
                break
        else:
            self.shared.stop()
            hist = self.deserialize(utx_hist)
            raise BaseException("prevout not found", addr, hist, txi.encode('hex'))

        self.batch_addr[addr] = utx_hist
        # self.batch_keys[spent_key] = spent_hist + new_item




    def serialize(self, h):
        s = ''
        for txid, txpos, value, height in h:
            s += self.serialize_item(txid, txpos, value, height)
        return s

    def serialize_item(self, txid, txpos, value, height, spent=chr(0)):
        s = (txid + int_to_hex(txpos, 4) + int_to_hex(value, 8) + int_to_hex(height, 3)).decode('hex') + spent 
        return s

    def deserialize_item(self,s):
        txid = s[0:32].encode('hex')
        txpos = int(rev_hex(s[32:36].encode('hex')), 16)
        value = int(rev_hex(s[36:44].encode('hex')), 16)
        height = int(rev_hex(s[44:47].encode('hex')), 16)
        spent = s[47:48]
        return (txid, txpos, value, height, spent)

    def deserialize(self, s):
        h = []
        while s:
            txid, txpos, value, height, spent = self.deserialize_item(s[0:48])
            h.append((txid, txpos, height))
            #if spent == chr(1):
            #    txid, txpos, value, height, spent = self.deserialize_item(s[48:96])
            #    h.append((txid, txpos, height))
            s = s[48:]
        return h

    def get_unspent(self, s):
        h = []
        while s:
            txid, txpos, value, height, spent = self.deserialize_item(s[0:48])
            if spent == chr(0):
                h.append((txid, txpos, value, height, s[0:44]))
            s = s[96:]
        return h



