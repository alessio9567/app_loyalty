import Logger
import EmailManager

class Process:

    def __init__(self, file_type):
        self.file_type = file_type
        self.logger = Logger.getLogger()
        self.email_title = "Process file "  + file_type
        self.email_body = ""
        self.inizio_esecuzione = 'START Process ' + file_type
        self.fine_esecuzione = 'END Process ' + file_type
        self.inizio_lettura = 'Inizio lettura file ' + file_type
        self.fine_lettura_ok = 'Terminata lettura file ' + file_type + ' con record validi'
        self.fine_lettura_ko = 'Terminata lettura file ' + file_type + ' senza record validi'
        self.inizio_elaborazione = 'Inizio elaborazione ' + file_type
        self.fine_elaborazione = 'Terminata elaborazione ' + file_type
        self.fine_elaborazione_no_record = 'Terminata elaborazione ' + file_type + ' senza record validi'
        self.esecuzione_fallita = '\n +++++ ERROR ESECUZIONE FALLITA +++++\n'
        self.inizio_refresh = 'Inizio refresh ' + file_type
        self.fine_refresh_ok = 'Terminato refresh ' + file_type
        self.fine_refresh_ko = 'Fallito refresh ' + file_type


    def mail_writer(self, text):
        self.email_body += (text + '\n')
        self.logger.info(text)

    def execute_all(self, Read, Elaboration, Refresh):
        self.mail_writer(self.inizio_esecuzione)
        try:
            self.mail_writer(self.inizio_lettura)
            result_read = Read.execute()

            if result_read:
                self.mail_writer(self.fine_lettura_ok)
                self.mail_writer(self.inizio_esecuzione)
                result_elaboration = Elaboration.execute()
                if result_elaboration:
                    self.mail_writer(self.fine_elaborazione)
                    self.mail_writer(self.inizio_refresh)
                    result_refresh = Refresh.execute()
                    if result_refresh:
                        self.mail_writer(self.fine_refresh_ok)
                    else:
                        self.mail_writer(self.fine_refresh_ko)
                else:
                    self.mail_writer(self.fine_elaborazione_no_record)
            else:
                self.mail_writer(self.fine_lettura_ko)
        except:
            self.logger.error(self.esecuzione_fallita)
            self.mail_writer(self.esecuzione_fallita)

        self.logger.info(self.email_title)
        self.logger.info(self.email_body)
        self.mail_writer(self.fine_esecuzione)
        EmailManager.sendEmailWithAttachment(self.email_title, self.email_body)


    def execute_only_read(self, Read):
        self.mail_writer(self.inizio_esecuzione)
        try:
            self.mail_writer(self.inizio_lettura)
            result_read = Read.execute()
            if result_read:
                self.mail_writer(self.fine_lettura_ok)
            else:
                self.mail_writer(self.fine_lettura_ko)
        except:
            self.logger.error(self.esecuzione_fallita)
            self.mail_writer(self.esecuzione_fallita)

        self.logger.info(self.email_title)
        self.logger.info(self.email_body)
        self.mail_writer(self.fine_esecuzione)
        EmailManager.sendEmailWithAttachment(self.email_title, self.email_body)

    def execute_only_elaboration(self, Elaboration):
        self.mail_writer(self.inizio_esecuzione)
        try:
            result_elaboration = Elaboration.execute()
            if result_elaboration:
                self.mail_writer(self.fine_elaborazione)
            else:
                self.mail_writer(self.fine_elaborazione_no_record)
        except:
            self.logger.error(self.esecuzione_fallita)
            self.mail_writer(self.esecuzione_fallita)

        self.logger.info(self.email_title)
        self.logger.info(self.email_body)
        self.mail_writer(self.fine_esecuzione)
        EmailManager.sendEmailWithAttachment(self.email_title, self.email_body)


    def execute_only_refresh(self, Refresh):
        self.mail_writer(self.inizio_esecuzione)
        try:
            self.mail_writer(self.inizio_refresh)
            result_refresh = Refresh.execute()
            if result_refresh:
                self.mail_writer(self.fine_refresh_ok)
            else:
                self.mail_writer(self.fine_refresh_ko)
        except:
            self.logger.error(self.esecuzione_fallita)
            self.mail_writer(self.esecuzione_fallita)

        self.logger.info(self.email_title)
        self.logger.info(self.email_body)
        self.mail_writer(self.fine_esecuzione)
        EmailManager.sendEmailWithAttachment(self.email_title, self.email_body)

    def execute_from_elaboration(self,  Elaboration, Refresh):
        self.mail_writer(self.inizio_esecuzione)
        try:
            self.mail_writer(self.inizio_esecuzione)
            result_elaboration = Elaboration.execute()
            if result_elaboration:
                self.mail_writer(self.fine_elaborazione)
                self.mail_writer(self.inizio_refresh)
                result_refresh = Refresh.execute()
                if result_refresh:
                    self.mail_writer(self.fine_refresh_ok)
                else:
                    self.mail_writer(self.fine_refresh_ko)
            else:
                self.mail_writer(self.fine_elaborazione_no_record)
        except:
            self.logger.error(self.esecuzione_fallita)
            self.mail_writer(self.esecuzione_fallita)

        self.logger.info(self.email_title)
        self.logger.info(self.email_body)
        self.mail_writer(self.fine_esecuzione)
        EmailManager.sendEmailWithAttachment(self.email_title, self.email_body)
