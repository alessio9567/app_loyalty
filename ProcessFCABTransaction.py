from loyalty_fca_utility import Process
from loyalty_fca_utility import ReadFCAB
from loyalty_fca_utility import ElaborationFCAB
from loyalty_fca_utility import RefreshFCAB
import sys

if __name__ == '__main__':

    valore_arg = ''
    if len(sys.argv) > 1:
        valore_arg = sys.argv[1]
    process = Process.Process('FCAB')

    if valore_arg == '':
        process.execute_all(ReadFCAB, ElaborationFCAB, RefreshFCAB)
    elif valore_arg == '1':
        process.execute_only_read(ReadFCAB)
    elif valore_arg == '2':
        process.execute_only_elaboration(ElaborationFCAB)
    elif valore_arg == '3':
        process.execute_only_refresh(RefreshFCAB)
    elif valore_arg == '4':
        process.execute_from_elaboration(ElaborationFCAB, RefreshFCAB)