from __future__ import print_function
import sys

class InMemoryDB(object):
    """
    The class object for the in-memory database.
    """
    def __init__(self):
        """
        The "table" serves as a "key-value" container. The
        "value_count" counts the number of distinguished
        keys that correspond to the same values. "block_stack"
        is a in-memory log for rollback purpose.
        """

        # The key of the table is a string while the value
        # of the table is a list. e.g. if we do "SET a 10"
        # and then "SET a 20", then the key is 'a' while
        # the value is [10, 20]. When we call "GET a", the
        # function retrieves the last value of the list as
        # the return value. For the same key we keep track of
        # all previous values so that they can be rollbacked
        # later.
        self.table = {}
        self.value_count = {}
        self.block_stack = []

    def put(self, key, value, rollback=False):
        """
        This funtion does what a 'SET' call will do, stores
        a key-value pair in the map.
        """
        table, block_stack = self.table, self.block_stack
        value_count = self.value_count

        # The put operation will check the in-memory log,
        # "block_stack", to find the most recent block and
        # append the current put transaction to the end of that 
        # block. However, if this function is triggered by
        # rollback, then this part will be skipped.
        if block_stack != [] and not rollback:
            block = block_stack[-1]
            block.append(('SET', key, value))

        # Update the map of "value_count".
        if value not in value_count:
            value_count[value] = 1
        else:
            value_count[value] += 1

        # Put a "key-value" pair in the map.
        if key in table:
            old_value = table[key][-1]
            value_count[old_value] -= 1
            if value_count[old_value] == 0:
                del value_count[old_value]
            table[key].append(value)
        else:
            table[key] = [value]

    def get(self, key):
        """
        Retrieves the most recent value in the "key-value"
        table, otherwise, returns NULL.
        """    
        if key in self.table:
            # Only retrieves the most recent value in the
            # value list.
            result = self.table[key][-1]
            return result if result != None else 'NULL'
        else:
            return 'NULL'

    def unset(self, key, rollback=False):
        """
        This function will restore the previous value if it's
        triggered by rollback. Otherwise, it deletes the key-value
        pair from the map.
        """
        table, block_stack = self.table, self.block_stack
        value_count = self.value_count

        if key in table:
            if block_stack != [] and not rollback:
                block = block_stack[-1]
                unset_value = table[key][-1]
                block.append(('UNSET', key, unset_value))

            ## Update count map.
            value = table[key][-1]
            if value in value_count:
                value_count[value] -= 1
                if value_count[value] == 0:
                    del value_count[value]
 
            # If triggered by rollback, then pop of the most
            # recent value corresponding to that key.
            if rollback:
                if len(table[key]) > 1:
                    table[key].pop()
                else:
                    del table[key]
            else:
                del table[key]

    def num_equal_to(self, value):
        """
        Returnes the number of keys that have the same
        value.
        """
        return self.value_count.get(value, 0)

    def initiate_block(self):
        """
        This function is called when a 'BEGIN' is read in 
        from command line.
        """
        self.block_stack.append([])

    def roll_back(self):
        """
        Roll back the most recent transaction block.        
        """
        if self.block_stack == []:
            print ('NO TRANSACTION')
            return

        roll_back_block = self.block_stack.pop()
        roll_back_block.reverse()

        for operation, key, value in roll_back_block:
            if operation == 'UNSET':
                self.put(key, value, rollback=True)
            elif operation == 'SET':
                self.unset(key, rollback=True)

    def commit(self):
        """
        Clear the in-memory log so that the transactions are made
        permenant. 
        """
        del self.block_stack[:]


    def take_transaction(self, command):
        """
        Read in a command in a line. 
        """
        operation, key, value = parse_transaction(command)

        return {
            'SET' :        lambda self, k, v: self.put(k, v),
            'UNSET' :      lambda self, k, v: self.unset(k),
            'GET' :        lambda self, k, v: print (self.get(k)),
            'NUMEQUALTO' : lambda self, k, v: print (self.num_equal_to(k)),
            'BEGIN' :      lambda self, k, v: self.initiate_block(),
            'ROLLBACK' :   lambda self, k, v: self.roll_back(),
            'COMMIT' :     lambda self, k, v: self.commit(),
            'END' :        lambda self, k, v: sys.exit(0)
        } [operation] (self, key, value)


def parse_transaction(line):
    """
    Break an entire command into a list of keywords. If the length
    of the list is below three, then fill it with 'None'.
    """
    transaction = [ word.strip(' \n') for word in line.split() ]
    while len(transaction) < 3:
        transaction.append(None)
    return transaction



