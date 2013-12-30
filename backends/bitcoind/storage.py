import plyvel, ast, hashlib, traceback, os
from processor import print_log
from utils import *


"""
todo: 
 * store spent histories 
 * miners
"""


DEBUG = 0


class Storage(object):

    def __init__(self, config, shared, test_reorgs):
        # address: 20 bytes + 1 (we don't need to add that byte)

        self.dbpath = config.get('leveldb', 'path_newtree')
        if not os.path.exists(self.dbpath):
            os.mkdir(self.dbpath)
        self.pruning_limit = config.getint('leveldb', 'pruning_limit')
        self.shared = shared
        self.hash_list = {}
        self.parents = {}

        self.test_reorgs = test_reorgs
        try:
            self.db_addr = plyvel.DB(os.path.join(self.dbpath,'addr'), create_if_missing=True, compression=None)
            self.db = plyvel.DB(os.path.join(self.dbpath,'utxo'), create_if_missing=True, compression=None)
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
            # write root
            self.put_node('', {})

        # check version
        if self.db_version != db_version:
            print_log("Your database '%s' is deprecated. Please create a new database"%self.dbpath)
            self.shared.stop()
            return



    # convert between bitcoin addresses and 20 bytes keys used for storage. 
    def address_to_key(self, addr):
        return bc_address_to_hash_160(addr)

    def key_to_address(self, addr):
        return hash_160_to_bc_address(addr)


    def get_history(self, addr):
        addr = self.address_to_key(addr)
        x = self.db_addr.get(addr)
        if x is None: 
            return ''
        try:
            _hash, v, h = x
            return h
        except:
            traceback.print_exc(file=sys.stdout)
            self.shared.stop()
            raise


    def get_address(self, txi):
        txi = 'b' + txi
        addr = self.db.get(txi)
        return self.key_to_address(addr) if addr else None


    def get_undo_info(self, height):
        s = self.db.get("undo%d" % (height % 100))
        if s is None: print_log("no undo info for ", height)
        return eval(s)


    def write_undo_info(self, height, bitcoind_height, undo_info):
        if height > bitcoind_height - 100 or self.test_reorgs:
            self.db.put("undo%d" % (height % 100), repr(undo_info))


    def common_prefix(self, word1, word2):
        max_len = min(len(word1),len(word2))
        for i in range(max_len):
            if word2[i] != word1[i]:
                index = i
                break
        else:
            index = max_len
        return word1[0:index]


    def put_node(self, key, d, batch=None):
        k = 0
        serialized = ''
        for i in range(256):
            if chr(i) in d.keys():
                k += 1<<i
                h, v = d[chr(i)]
                if h is None: h = chr(0)*32
                vv = int_to_hex(v, 8).decode('hex')
                item = h + vv
                assert len(item) == 40
                serialized += item

        k = "0x%0.64X" % k # 32 bytes
        k = k[2:].decode('hex')
        assert len(k) == 32
        out = k + serialized
        if batch:
            batch.put(key, out)
        else:
            self.db_addr.put(key, out) 


    def get_node(self, key):

        s = self.db_addr.get(key)
        if s is None: return 
        k = int(s[0:32].encode('hex'), 16)
        s = s[32:]
        d = {}
        for i in range(256):
            if k % 2 == 1: 
                _hash = s[0:32]
                value = hex_to_int(s[32:40])
                d[chr(i)] = (_hash, value)
                s = s[40:]
            k = k/2

        #cache
        return d


    def add_address(self, target, serialized_hist):

        word = target
        key = ''
        path = [ '' ]
        i = self.db_addr.iterator()

        while key != target:

            items = self.get_node(key)

            if word[0] in items.keys():
  
                i.seek(key + word[0])
                new_key, _ = i.next()

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
                    index = len(prefix)

                    ## get hash and value of new_key from parent (if it's a leaf)
                    if len(new_key) == 20:
                        parent_key = self.get_parent(new_key)
                        parent = self.get_node(parent_key)
                        z = parent[ new_key[len(parent_key)] ]
                        self.put_node(prefix, { target[index]:(None,0), new_key[index]:z } )
                    else:
                        # if it is not a leaf, update the hash of new_key because skip_string changed
                        h, v = self.get_node_hash(new_key, self.get_node(new_key), prefix)
                        self.put_node(prefix, { target[index]:(None,0), new_key[index]:(h,v) } )

                    path.append(prefix)
                    self.parents[new_key] = prefix
                    break

            else:
                assert key in path
                items[ word[0] ] = (None,0)
                self.put_node(key,items)
                break

        # write 
        self.db_addr.put(target, serialized_hist)

        # compute hash and value of the final node
        utxo = self.get_unspent(serialized_hist)
        _hash = self.hash_tree(map( lambda x:x[4], utxo)) if utxo else None
        assert len(_hash) == 32
        value = sum( map( lambda x:x[2], utxo ) )
        self.update_node_hash(target, path, _hash, value)


    def update_node_hash(self, node, path, _hash, value):
        c = node
        for x in path[::-1]:
            self.parents[c] = x
            c = x

        self.hash_list[node] = (_hash, value)


    def update_hashes(self):
        nodes = {} # nodes to write

        for i in range(20, -1, -1):

            for node in self.hash_list.keys():
                if len(node) != i: continue

                node_hash, node_value = self.hash_list.pop(node)

                # for each node, compute its hash, send it to the parent
                if node == '':
                    self.root_hash = node_hash
                    self.root_value = node_value
                    break

                parent = self.parents[node]

                # read parent.. do this in add_address
                d = nodes.get(parent)
                if d is None:
                    d = self.get_node(parent)
                    assert d is not None

                letter = node[len(parent)]
                assert letter in d.keys()

                if i!=20 and node_hash is None:
                    d2 = self.get_node(node)
                    try:
                        node_hash, node_value = self.get_node_hash(node, d2, parent)
                    except:
                        print node_hash, node_value
                        raise

                assert node_hash is not None
                # write new value
                d[letter] = (node_hash, node_value)
                nodes[parent] = d

                # iterate
                grandparent = self.parents[parent] if parent != '' else None
                parent_hash, parent_value = self.get_node_hash(parent, d, grandparent)
                self.hash_list[parent] = (parent_hash, parent_value)

        
        # batch write modified nodes 
        batch = self.db_addr.write_batch()
        for k, v in nodes.items():
            self.put_node(k, v, batch)
        batch.write()

        # cleanup
        assert self.hash_list == {}
        self.parents = {}


    def get_node_hash(self, x, d, parent):

        # final hash
        if x != '':
            skip_string = x[len(parent)+1:]
        else:
            skip_string = ''

        d2 = sorted(d.items())
        values = map(lambda x: x[1][1], d2)
        hashes = map(lambda x: x[1][0], d2)
        value = sum( values )
        _hash = self.hash( skip_string + ''.join(hashes) )
        return _hash, value


    def get_path(self, target):
        word = target
        key = ''
        path = [ '' ]
        i = self.db_addr.iterator(start='')

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
                    print_log('not in tree', self.db_addr.get(key+word[0]), new_key.encode('hex'))
                    return False
            else:
                assert key in path
                break

        return path


    def delete_address(self, addr):
        path = self.get_path(addr)
        if path is False:
            print_log("addr not in tree", addr.encode('hex'), self.key_to_address(addr), self.db_addr.get(addr))
            raise

        self.db_addr.delete(addr)
        if addr in self.hash_list:
            self.hash_list.pop(addr)

        parent = path[-1]
        letter = addr[len(parent)]
        items = self.get_node(parent)
        items.pop(letter)

        # remove key if it has a single child
        if len(items) == 1:
            letter, v = items.items()[0]
            _hash, value = v
            self.db_addr.delete(parent)
            if parent in self.hash_list: 
                #print "zz"
                self.hash_list.pop(parent)

            # we need the exact length for the iteration
            i = self.db_addr.iterator()
            i.seek(parent+letter)
            k, v = i.next()
            # note: k is not necessarily a leaf
            self.update_node_hash(k, path[:-1], _hash, value)

        else:
            self.put_node(parent, items)
            #grandparent = path[-2] if parent !='' else None  # is grandparent stable?
            #_hash, value = self.get_node_hash(parent, items, grandparent)
            _hash, value = None, 'zz'
            # do not propagate hash if it has been deprecated by a child
            #if k in self.parents.keys(): _hash = None

            self.update_node_hash(parent, path[:-1], _hash, value)



    def get_children(self, x):
        i = self.db_addr.iterator()
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




    def hash_tree(self, base_list):
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
        i = self.db_addr.iterator()
        for j in range(len(x)):
            p = x[0:-j-1]
            i.seek(p)
            k, v = i.next()
            if x.startswith(k) and x!=k: 
                break
        else: raise
        return k

        
    def hash(self, x):
        if DEBUG: return "hash("+x+")"
        return Hash(x)

    def get_root_hash(self):
        return self.root_hash


    def print_all(self):
        i = self.db_addr.iterator()
        for k, v in i:
            if k:
                addr = k.encode('hex')
                if len(addr)<20:
                    items = ast.literal_eval(v)
                    print addr, "->", items
                else:
                    print addr, "->", v.encode('hex')
        print " ---------- "

    def close(self):
        self.db_addr.close()
        self.db.close()


    def add_to_history(self, addr, tx_hash, tx_pos, value, tx_height):
        "add tx output to the history of addr"
        "also creates a backlink: txo-> addr"

        addr = self.address_to_key(addr)

        node = self.db_addr.get(addr)
        if node:
            serialized_hist = node
        else: 
            serialized_hist = ''

        # keep it sorted
        s = self.serialize_item(tx_hash, tx_pos, value, tx_height)
        l = len(serialized_hist)/48
        for i in range(l-1, -1, -1):
            item = serialized_hist[48*i:48*(i+1)]
            item_height = int(rev_hex(item[44:47].encode('hex')), 16)
            if item_height <= tx_height:
                serialized_hist = serialized_hist[0:48*(i+1)] + s + serialized_hist[48*(i+1):]
                break
        else:
            serialized_hist = s + serialized_hist


        # write the new history
        self.add_address(addr, serialized_hist)


        # backlink
        txo = (tx_hash + int_to_hex(tx_pos, 4)).decode('hex')
        self.db.put('b'+txo, addr)




    def revert_add_to_history(self, addr, tx_hash, tx_pos, value, tx_height):
        addr = self.address_to_key(addr)

        serialized_hist = self.db_addr.get(addr)

        s = self.serialize_item(tx_hash, tx_pos, value, tx_height)
        if serialized_hist.find(s) == -1: raise
        serialized_hist = serialized_hist.replace(s, '')

        if serialized_hist:
            #print "revert add_address" 
            self.add_address(addr, serialized_hist)
        else:
            #print "revert, delete_address"
            self.delete_address(addr)

        # backlink
        txo = (tx_hash + int_to_hex(tx_pos, 4)).decode('hex')
        self.db.delete('b'+txo)




    def revert_set_spent(self, addr, txi, undo):
        addr = self.address_to_key(addr)

        # restore backlink
        self.db.put('b' + txi, addr)

        # restore removed items
        if undo.get(addr) is not None: 
            itemlist = undo.pop(addr)
        else:
            return 

        if not itemlist: return

        node = self.db_addr.get(addr)
        if node:
            serialized_hist = node
        else:
            serialized_hist = ''

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


        self.add_address(addr, serialized_hist)






    def set_spent(self, addr, txi, txid, index, height, undo):
        addr = self.address_to_key(addr)
        if undo.get(addr) is None: undo[addr] = []

        utx_hist = self.db_addr.get(addr)

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

        if utx_hist:
            self.add_address(addr, utx_hist)
        else:
            self.delete_address(addr)

        # delete backlink txi-> addr
        self.db.delete('b'+txi)



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
            assert spent == chr(0)
            h.append((txid, txpos, value, height, Hash(s[0:44])))
            s = s[48:]
        return h

        

    def import_transaction(self, txid, tx, block_height, touched_addr):

        undo = { 'prev_addr':[] } # contains the list of pruned items for each address in the tx; also, 'prev_addr' is a list of prev addresses
                
        prev_addr = []
        for i, x in enumerate(tx.get('inputs')):
            txi = (x.get('prevout_hash') + int_to_hex(x.get('prevout_n'), 4)).decode('hex')
            addr = self.get_address(txi)
            # Add redeem item to the history.
            if addr is not None: 
                self.set_spent(addr, txi, txid, i, block_height, undo)
                touched_addr.add(addr)
            prev_addr.append(addr)

        undo['prev_addr'] = prev_addr 

        # here I add only the outputs to history; maybe I want to add inputs too (that's in the other loop)
        for x in tx.get('outputs'):
            addr = x.get('address')
            if addr is None: continue
            self.add_to_history(addr, txid, x.get('index'), x.get('value'), block_height)
            touched_addr.add(addr)

        return undo


    def revert_transaction(self, txid, tx, block_height, touched_addr, undo):
        for x in tx.get('outputs'):
            addr = x.get('address')
            if addr is None: continue
            self.revert_add_to_history(addr, txid, x.get('index'), x.get('value'), block_height)
            touched_addr.add(addr)

        prev_addr = undo.pop('prev_addr')
        for i, x in enumerate(tx.get('inputs')):
            addr = prev_addr[i]
            if addr is not None:
                txi = (x.get('prevout_hash') + int_to_hex(x.get('prevout_n'), 4)).decode('hex')
                self.revert_set_spent(addr, txi, undo)
                touched_addr.add(addr)

        assert undo == {}

