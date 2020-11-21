from loyalty_fca_utility import Process
from loyalty_fca_utility import ReadNFR
from loyalty_fca_utility import ElaborationNFR
from loyalty_fca_utility import RefreshNFR
import sys

if __name__ == '__main__':

    valore_arg = ''
    if len(sys.argv) > 1:
        valore_arg = sys.argv[1]
    process = Process.Process('NFR')

    if valore_arg == '':
        process.execute_all(ReadNFR, ElaborationNFR, RefreshNFR)
    elif valore_arg == '1':
        process.execute_only_read(ReadNFR)
    elif valore_arg == '2':
        process.execute_only_elaboration(ElaborationNFR)
    elif valore_arg == '3':
        process.execute_only_refresh(RefreshNFR)
    elif valore_arg == '4':
        process.execute_from_elaboration(ElaborationNFR, RefreshNFR)
