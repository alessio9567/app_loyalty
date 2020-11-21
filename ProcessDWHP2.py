from loyalty_fca_utility import Process
from loyalty_fca_utility import ReadDWHP2
from loyalty_fca_utility import ElaborationDWHP2
from loyalty_fca_utility import RefreshDWHP2
import sys

if __name__ == '__main__':

    valore_arg = ''
    if len(sys.argv) > 1:
        valore_arg = sys.argv[1]
    process = Process.Process('DWHP2')

    if valore_arg == '':
        process.execute_all(ReadDWHP2, ElaborationDWHP2, RefreshDWHP2)
    elif valore_arg == '1':
        process.execute_only_read(ReadDWHP2)
    elif valore_arg == '2':
        process.execute_only_elaboration(ElaborationDWHP2)
    elif valore_arg == '3':
        process.execute_only_refresh(RefreshDWHP2)
    elif valore_arg == '4':
        process.execute_from_elaboration(ElaborationDWHP2, RefreshDWHP2)
