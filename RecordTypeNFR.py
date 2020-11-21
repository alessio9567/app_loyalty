# funzioni per leggere record file GLF
import MainframeFieldsUtility
from pyspark.sql import Row

#####################################################################
###################### Struttura tabella NFR ########################
#####################################################################
#value = 'value'
#alias = 'alias'
#
#tableStructure = [
#  {'appcode' : {value : '', alias : ''}},                          # varchar(30) comment 'e.g. CAMS', 
#  {'issuer_id' : {value : '', alias : ''}},                        # varchar(9) comment 'from header e.g. FCAB18210', 
#  {'customer_company' : {value : '', alias : 'X11422PC'}},                 # char(5) comment 'X11422PC FCA Company', 
#  {'product_company' : {value : '', alias : 'X11422QZ'}},                  # char(5) comment 'X11422QZ  e.g. 18210', 
#  {'customer_number' : {value : '', alias : 'X11422W0'}},                  # varchar(15) comment 'X11422W0 9(15) Customer: identified by the customer number linked to each member account', 
#  {'account_number' : {value : '', alias : 'X1142237'}},                   # varchar(23) comment 'X1142237 Member account: identified by the account number', 
#  {'loyalty_program_cod' : {value : '', alias : 'X11422FS'}},              # char(6) comment 'X11422FS Member account: identified by the account number', 
#  {'loyalty_program_status' : {value : '', alias : 'X11422BI'}},           # char(3) comment 'X11422BI', 
#  {'is_customer_number_active' : {value : '', alias : ''}},        # boolean comment 'logic delete', 
#  {'is_account_number_active' : {value : '', alias : ''}},         # boolean comment 'logic delete', 
#  {'customer_status' : {value : '', alias : ''}},                  # varchar(100), 
#  {'account_status' : {value : '', alias : ''}},                   # varchar(100), 
#  {'customer_name' : {value : '', alias : 'X1142239'}},                    # varchar(40) comment 'X1142239', 
#  {'customer_address' : {value : '', alias : 'X1142241'}},                 # varchar(40) comment 'X1142241', 
#  {'customer_city' : {value : '', alias : 'X1142244'}},                    # varchar(25) comment 'X1142244', 
#  {'customer_state' : {value : '', alias : 'X1142245'}},                   # varchar(15) comment 'X1142245 CustomerState or CustomerProvince', 
#  {'zip_cod' : {value : '', alias : 'X1142246'}},                          # varchar(9) comment 'X1142246', 
#  {'country_cod' : {value : '', alias : 'X1142250'}},                      # char(2) comment 'X1142250', 
#  {'date_birth' : {value : '', alias : 'X1142247'}},                       # int comment 'X1142247 format yyyyMMdd', 
#  {'home_tel' : {value : '', alias : 'X1142248'}},                         # varchar(11) comment 'X1142248', 
#  {'work_tel' : {value : '', alias : 'X1142249'}},                         # varchar(16) comment 'X1142249', 
#  {'taxid_number' : {value : '', alias : 'X1142251'}},                     # varchar(11) comment 'X1142251', 
#  {'document_type' : {value : '', alias : 'X1142252'}},                    # char(1) comment 'X1142252', 
#  {'city_birth' : {value : '', alias : 'X1142253'}},                       # varchar(25) comment 'X1142253', 
#  {'gender' : {value : '', alias : 'X1142254'}},                           # char(1) comment 'X1142254', 
#  {'nationality' : {value : '', alias : 'X11422BS'}},                      # char(2) comment 'X11422BS', 
#  {'email' : {value : '', alias : 'X11422UW'}},                            # varchar(40) comment 'X11422UW', 
#  {'input_filename' : {value : '', alias : ''}},                   # varchar(100),
#  {'input_filename_timestamp' : {value : '', alias : ''}},         # bigint comment 'partition',
#  {'elaboration_ts' : {value : '', alias : ''}},         # , 
#  {'internallog_ts' : {value : '', alias : ''}}          #
#]


def getNFRvalidKeys():
    validKey = [
        'X11422QZ', # product company, is this the same in the header?
        'X1142237', # accountNumber
        'X11422W0', # customerNumber
        'X11422PC', # customerCompany 
        'X1142239', # CustomerName
        'X1142241', # CustomerAddress, 
        'X1142244', # CustomerCity
        'X1142245', # CustomerState/Province
        'X1142246', # ZipCode
        'X1142250', # CountryCode
        'X1142247', # DateOfBirth
        'X1142248', # Home Telephone
        'X1142249', # WorkTelephone
        'X1142251', # CodiceFiscale
        'X1142252', # tipo documento
        'X1142253', # city of birth 
        'X1142254', # sesso
        'X11422BS', # nationality
        'X11422UW', # email address? 
        'X11422FS', # LoyaltyProgram
        'X11422BI'
        ] # LoyaltyProgram Status Code
    return validKey



def isFileHeader( stringa ):
   if stringa[9:10] == 'H':
        return True 
   else :
        return False

def mapHeaderRow( stringa ):
        row = {
            "issuerID" : stringa[0:9].encode('utf-8').strip(), 
            "headerIndicator" : stringa[9:10].encode('utf-8').strip(),
            "senderID" : stringa[10:19].encode('utf-8').strip(),
            "batchID" : stringa[19:33].encode('utf-8').strip(), # timestamp, same value at the begining of DETAIL RECORDS
            "recordCounter" : stringa[33:42].encode('utf-8').strip(), # numero di record presenti nel file
            # filler lungo 34
            "senderCode" : stringa[76:81].encode('utf-8').strip(),
            "companyCAMS" : stringa[81:86].encode('utf-8').strip(),
            "owner1" : stringa[86:91].encode('utf-8').strip(),
            "owner2" : stringa[91:96].encode('utf-8').strip(),
            "owner3" : stringa[96:101].encode('utf-8').strip(),
            "sequenceNumber" : stringa[101:110].encode('utf-8').strip(), 
            "header" : stringa[110:117].encode('utf-8').rstrip('\n').rstrip('\r\n')
            # filler lungo  
        }
        return row

def mapDetailRow( stringa ):
        row = {
            "issuerBatchId" : stringa[0:14].encode('utf-8').strip(),
            "ibrKey" : stringa[14:25].encode('utf-8').strip(),
            "functionID" : stringa[25:35].encode('utf-8').strip(), # nome del PGM, come RECUCARD, non interessante?
            "result" : stringa[35:36].encode('utf-8').strip(), # valore P o N
            "keyOfFunction" : stringa[36:59].encode('utf-8').strip(),
            "keyType" : stringa[59:60].encode('utf-8').strip(),
            "aliasID" : stringa[60:68].encode('utf-8').strip(),
            "value" : stringa[68:200].strip().rstrip('\n').rstrip('\r\n')
        }
        return row

def setCustomerProperties( customers, keyCustomers, keyCustomerProperties, valueCustomerProperties ):
    if keyCustomers in customers.keys()  :
        customerProperties = customers[keyCustomers]
        customerProperties[keyCustomerProperties] = valueCustomerProperties
        customers[keyCustomers] = customerProperties
        return customers
    else :
        customerProperties = dict()
        customerProperties[keyCustomerProperties] = valueCustomerProperties
        customers[keyCustomers] = customerProperties
        return customers

def getDictKey(dizionario, chiave):
    if(chiave in dizionario.keys()):
        return dizionario[chiave]
    else:
        return ''

def setNfrTable( customers , APPLICATION_CODE, input_filename, input_filename_timestamp, elaboration_ts, internallog_ts, RowOrList = 'R'):
    rowList = list()
    rowRow = list()
    for accountNumber in customers.keys()  :  

        name = ''
        surname = ''

        if '*' in getDictKey(customers[accountNumber], 'X1142239'): 
            nameSurname = getDictKey(customers[accountNumber], 'X1142239').split('*')
            name = nameSurname[0].strip()
            surname = nameSurname[1].strip()
        else:
            name = getDictKey(customers[accountNumber], 'X1142239')

        #print accountNumber
        rowList.append((
        APPLICATION_CODE     
        , ''
        , getDictKey(customers[accountNumber], 'X11422PC') 
        , getDictKey(customers[accountNumber], 'X11422QZ') 
        , getDictKey(customers[accountNumber], 'X11422W0') 
        , getDictKey(customers[accountNumber], 'X1142237')  
#        , getDictKey(customers[accountNumber], 'X11422FS')     
#        , getDictKey(customers[accountNumber], 'X11422BI')    
        , True  
        , True  
        , ''   
        , ''  
        , name  # name          
        , surname  # surname        
        , getDictKey(customers[accountNumber], 'X1142241')   
        , getDictKey(customers[accountNumber], 'X1142244')   
        , getDictKey(customers[accountNumber], 'X1142245')   
        , getDictKey(customers[accountNumber], 'X1142246')   
        , getDictKey(customers[accountNumber], 'X1142250')   
        , MainframeFieldsUtility.read_CYYMMDD_date(getDictKey(customers[accountNumber], 'X1142247'))  
        , getDictKey(customers[accountNumber], 'X1142248')   
        , getDictKey(customers[accountNumber], 'X1142249')   
        , getDictKey(customers[accountNumber], 'X1142251')   
        , getDictKey(customers[accountNumber], 'X1142252')   
        , getDictKey(customers[accountNumber], 'X1142253')   
        , getDictKey(customers[accountNumber], 'X1142254')   
        , getDictKey(customers[accountNumber], 'X11422BS')   
        , getDictKey(customers[accountNumber], 'X11422UW').replace('\xc2\xa7', '@').replace('\xa7', '@')     
        , input_filename
        , input_filename_timestamp
        , elaboration_ts  
        , internallog_ts))
        # 
        rowRow.append(Row(
        appcode =               APPLICATION_CODE     
        , issuer_id =           ''
        , customer_company =    getDictKey(customers[accountNumber], 'X11422PC') 
        , product_company =     getDictKey(customers[accountNumber], 'X11422QZ') 
        , customer_number =     getDictKey(customers[accountNumber], 'X11422W0') 
        , account_number =      getDictKey(customers[accountNumber], 'X1142237')  
#        , loyalty_program_cod =         getDictKey(customers[accountNumber], 'X11422FS')     
#        , loyalty_program_status =   getDictKey(customers[accountNumber], 'X11422BI')    
        , is_customer_number_active = True  
        , is_account_number_active = True  
        , customer_status =         ''   
        , account_status =      ''  
        , customer_name =                name   
        , customer_surname =             surname  
        , customer_address =        getDictKey(customers[accountNumber], 'X1142241')   
        , customer_city =       getDictKey(customers[accountNumber], 'X1142244')   
        , customer_state =      getDictKey(customers[accountNumber], 'X1142245')   
        , zip_cod =                 getDictKey(customers[accountNumber], 'X1142246')   
        , country_cod =         getDictKey(customers[accountNumber], 'X1142250')   
        , date_birth =          MainframeFieldsUtility.read_CYYMMDD_date(getDictKey(customers[accountNumber], 'X1142247'))   # cambiare             
        , home_tel =                getDictKey(customers[accountNumber], 'X1142248')   
        , work_tel =            getDictKey(customers[accountNumber], 'X1142249')   
        , taxid_number =        getDictKey(customers[accountNumber], 'X1142251')   
        , document_type =       getDictKey(customers[accountNumber], 'X1142252')   
        , city_birth =          getDictKey(customers[accountNumber], 'X1142253')   
        , gender =              getDictKey(customers[accountNumber], 'X1142254')   
        , nationality =             getDictKey(customers[accountNumber], 'X11422BS')   
        , email =               getDictKey(customers[accountNumber], 'X11422UW').replace('\xc2\xa7', '@').replace('\xa7', '@')    
        , input_filename =          input_filename
        , input_filename_timestamp =    input_filename_timestamp
        , elaboration_ts =              elaboration_ts  
        , internallog_ts =              internallog_ts))
        #
    if RowOrList == 'R':
        return rowRow
    else:
        return rowList



