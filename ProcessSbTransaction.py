from loyalty_fca_utility import Process
from loyalty_fca_utility import ReadSbTransaction
from loyalty_fca_utility import ElaborationSbTransaction
from loyalty_fca_utility import RefreshSbTransaction
import sys

if __name__ == '__main__':

    valore_arg = ''
    if len(sys.argv) > 1:
        valore_arg = sys.argv[1]
    process = Process.Process('SbTransaction')

    if valore_arg == '':
        process.execute_all(ReadSbTransaction, ElaborationSbTransaction, RefreshSbTransaction)
    elif valore_arg == '1':
        process.execute_only_read(ReadSbTransaction)
    elif valore_arg == '2':
        process.execute_only_elaboration(ElaborationSbTransaction)
    elif valore_arg == '3':
        process.execute_only_refresh(RefreshSbTransaction)
    elif valore_arg == '4':
        process.execute_from_elaboration(ElaborationSbTransaction, RefreshSbTransaction)
