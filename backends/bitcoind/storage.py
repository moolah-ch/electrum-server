import plyvel, ast, hashlib, traceback
from processor import print_log
from utils import *


"""
todo: 
 * store spent histories 
 * miners

root hash:
@1000:  56fe80bfee7ae86e109254d197b26679e2ba5d808f7d856e47ea21b5ed2f2f17
@10000: 05a728d62caa3248a4b0b284b4a2e9492114d42eee297a9ed8f4bd279db823aa
@100000:c3bc43a7cae6aa8c1fcecbfa52d72ee1afe80634a1f1d72b3cf1dd9f37edf427
"""

DEBUG = False


class Storage(object):

    def __init__(self, config, shared, test_reorgs):
        # address: 20 bytes + 1 (we don't need to add that byte)

        self.dbpath = config.get('leveldb', 'path_hashtree')
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
            # write root
            self.put_node('a', '', 0, None)

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

        s = self.db_get(key)
        if s is None: 
            return 
        return self.parse_node(s)


    def get_address(self, txi):
        txi = 'b' + txi
        addr = self.db_get(txi)
        return self.key_to_address(addr) if addr else None

    def put(self, key, value):
        self.db.put(key, value)

    def delete(self, key):
        self.db.delete(key)

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



    def put_node(self, key, _hash, value, item):
        self.put(key, repr( ( _hash, value, item) ) )


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

        self.update_history(target, serialized_hist)


    def update_history(self, addr, serialized_hist):
        # compute hash and value of the final node
        utxo = self.get_unspent(serialized_hist)
        _hash = self.hash_tree(map( lambda x:x[4], utxo)) if utxo else None
        value = sum( map( lambda x:x[2], utxo ) )
        # write 
        self.put_node(addr, _hash, value, serialized_hist)

        # update hashes
        #for x in path[::-1]:
        #    self.update_node_hash(x)


    def update_all_hashes(self):

        for j in range(20, 0, -1):
            wb = self.db.write_batch()
            i = self.db.iterator(start='a', stop='b')
            for k, v in i:
                if len(k)==j:
                    _hash, value = self.update_node_hash(k)
                    wb.put(k, repr( ( _hash, value, None ) ) )
            wb.write()



    def get_path(self, target):
        assert target[0] == 'a'

        word = target[1:]
        key = 'a'
        path = [ 'a' ]
        i = self.db.iterator(start='a', stop='b')

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
                    print_log('not in tree', self.db.get(key+word[0]), new_key.encode('hex'))
                    return False
            else:
                assert key in path
                break

        return path


    def delete_address(self, addr):
        path = self.get_path(addr)
        if path is False:
            print_log("address not in tree", addr.encode('hex'), self.key_to_address(addr), self.db.get(addr))
            raise
            return

        self.delete(addr)

        p = path[-1]
        #remove key if it has a single child
        ch = self.get_children(p)
        if len([x for x in ch]) == 1:
            self.delete(p)



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
        i = self.db.iterator()

        # get the hashes of children
        hashes = []
        values = []
        ch = [ xx for xx in self.get_children(x)]

        for k,v in ch:
            _hash, v, _ = ast.literal_eval(v)
            try:
                assert _hash is not None
            except:
                print repr(x), repr(k)
                raise
            hashes.append(_hash)
            values.append(v)

        try:
            assert len(hashes) > 1
        except:
            print repr(x), ch, hashes
            raise
        value = sum( values )

        # final hash
        if x != 'a':
            parent, skip_string = self.get_parent(x)
        else:
            skip_string = ''

        _hash = self.hash( skip_string + ''.join(hashes) )

        return _hash, value

        #self.put_node(x, _hash, value, None)

        
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
        "add tx output to the history of addr"
        "also creates a backlink: txo-> addr"

        addr = self.address_to_key(addr)

        node = self.get_node(addr)
        if node:
            _, _, serialized_hist = node
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
        if not node:
            self.add_address(addr, serialized_hist)
        else:
            self.update_history(addr, serialized_hist)

        # backlink
        txo = (tx_hash + int_to_hex(tx_pos, 4)).decode('hex')
        self.put('b'+txo, addr)




    def revert_add_to_history(self, addr, tx_hash, tx_pos, value, tx_height):
        addr = self.address_to_key(addr)

        _, _, serialized_hist = self.get_node(addr)

        s = self.serialize_item(tx_hash, tx_pos, value, tx_height)
        if serialized_hist.find(s) == -1: raise
        serialized_hist = serialized_hist.replace(s, '')

        if serialized_hist:
            self.update_history(addr, serialized_hist)
        else:
            self.delete_address(addr)

        # backlink
        txo = (tx_hash + int_to_hex(tx_pos, 4)).decode('hex')
        self.delete('b'+txo)




    def revert_set_spent(self, addr, txi, undo):
        addr = self.address_to_key(addr)

        # restore backlink
        self.put('b' + txi, addr)

        # restore removed items
        if undo.get(addr) is not None: 
            itemlist = undo.pop(addr)
        else:
            return 

        if not itemlist: return

        node = self.get_node(addr)
        if node:
            _,_, serialized_hist = node
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


        if node:
            self.update_history(addr, serialized_hist)
        else:
            self.add_address(addr, serialized_hist)






    def set_spent(self, addr, txi, txid, index, height, undo):
        addr = self.address_to_key(addr)
        if undo.get(addr) is None: undo[addr] = []

        _,_,utx_hist = self.get_node(addr)

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
            self.update_history(addr, utx_hist)
        else:
            self.delete_address(addr)

        # delete backlink txi-> addr
        self.delete('b'+txi)



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
                touched_addr.append(addr)
            prev_addr.append(addr)

        undo['prev_addr'] = prev_addr 

        # here I add only the outputs to history; maybe I want to add inputs too (that's in the other loop)
        for x in tx.get('outputs'):
            addr = x.get('address')
            if addr is None: continue
            self.add_to_history(addr, txid, x.get('index'), x.get('value'), block_height)
            touched_addr.append(addr)

        return undo


    def revert_transaction(self, txid, tx, block_height, touched_addr, undo):
        for x in tx.get('outputs'):
            addr = x.get('address')
            if addr is None: continue
            self.revert_add_to_history(addr, txid, x.get('index'), x.get('value'), block_height)
            touched_addr.append(addr)

        prev_addr = undo.pop('prev_addr')
        for i, x in enumerate(tx.get('inputs')):
            addr = prev_addr[i]
            if addr is not None:
                txi = (x.get('prevout_hash') + int_to_hex(x.get('prevout_n'), 4)).decode('hex')
                self.revert_set_spent(addr, txi, undo)
                touched_addr.append(addr)

        assert undo == {}

