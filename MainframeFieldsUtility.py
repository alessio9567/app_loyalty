import Logger
import CurrenciesDict

logger = Logger.getLogger()

def sign_Handler( campo ):

    CAMPO_SEGNATO_ZERO_POSITIVO = '\xe0'
    CAMPO_SEGNATO_ZERO_NEGATIVO = '\xe8'

    carattere_segno = campo[-1:]
    campoParteFissa = campo[:-1]
    segno = ''
    valore = ''
    if carattere_segno == CAMPO_SEGNATO_ZERO_POSITIVO:
        segno = '+'
        valore = campoParteFissa + '0'
    elif carattere_segno == CAMPO_SEGNATO_ZERO_NEGATIVO:
        segno = '-'
        valore = campoParteFissa + '0'
    elif carattere_segno == 'A':
        segno = '+'
        valore = campoParteFissa + '1'
    elif carattere_segno == 'B':
        segno = '+'
        valore = campoParteFissa + '2'
    elif carattere_segno == 'C':
        segno = '+'
        valore = campoParteFissa + '3'
    elif carattere_segno == 'D':
        segno = '+'
        valore = campoParteFissa + '4'
    elif carattere_segno == 'E':
        segno = '+'
        valore = campoParteFissa + '5'
    elif carattere_segno == 'F':
        segno = '+'
        valore = campoParteFissa + '6'
    elif carattere_segno == 'G':
        segno = '+'
        valore = campoParteFissa + '7'
    elif carattere_segno == 'H':
        segno = '+'
        valore = campoParteFissa + '8'
    elif carattere_segno == 'I':
        segno = '+'
        valore = campoParteFissa + '9'
    elif carattere_segno == 'J':
        segno = '-'
        valore = campoParteFissa + '1'
    elif carattere_segno == 'K':
        segno = '-'
        valore = campoParteFissa + '2'
    elif carattere_segno == 'L':
        segno = '-'
        valore = campoParteFissa + '3'
    elif carattere_segno == 'M':
        segno = '-'
        valore = campoParteFissa + '4'
    elif carattere_segno == 'N':
        segno = '-'
        valore = campoParteFissa + '5'
    elif carattere_segno == 'O':
        segno = '-'
        valore = campoParteFissa + '6'
    elif carattere_segno == 'P':
        segno = '-'
        valore = campoParteFissa + '7'
    elif carattere_segno == 'Q':
        segno = '-'
        valore = campoParteFissa + '8'
    elif carattere_segno == 'R':
        segno = '-'
        valore = campoParteFissa + '9'
    else:
        logger.error('Errore! Carattere non riconosciuto')
        valore = 'ERRORE'
    valore_segno = {'valore': valore, 'segno': segno}
    return valore_segno



def read_CYYMMDD_date( data ):
    C = ''
    if data[0:1] == '0':
        C = '19'
    elif data[0:1] == '1':
        C = '20'
    elif data[0:1] == '2':
        C = '21'
    else:
        logger.error('ERRORE LETTURA DATA il valore ' + data[0:1])
        C = 'ER'
    
    data_yyyyMMdd = C + data[1:3] + data[3:5] + data[5:7]

    return data_yyyyMMdd


def translate_currency( currency_cod ):

    currencies = CurrenciesDict.getCurrencies()
    value = currencies.get(currency_cod)

    if(value == None):
        logger.error('ERRORE LETTURA CURRENCY non presente nel dizionario: ' + currency_cod)
        return 'EUR'
    return value





