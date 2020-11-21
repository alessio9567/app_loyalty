from loyalty_fca_utility import Process
from loyalty_fca_utility import ReadGLF
from loyalty_fca_utility import ElaborationGLF
from loyalty_fca_utility import RefreshGLF
import sys

if __name__ == '__main__':
    
    valore_arg = ''
    if len(sys.argv) > 1:
        valore_arg = sys.argv[1]
    process = Process.Process('GLF')

    if valore_arg == '':
        process.execute_all(ReadGLF, ElaborationGLF, RefreshGLF)
    elif valore_arg == '1':
        process.execute_only_read(ReadGLF)
    elif valore_arg == '2':
        process.execute_only_elaboration(ElaborationGLF)
    elif valore_arg == '3':
        process.execute_only_refresh(RefreshGLF)
    elif valore_arg == '4':
        process.execute_from_elaboration(ElaborationGLF, RefreshGLF)
    
