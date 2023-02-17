#!/usr/bin/env python
#_*_ codig: utf8 _*_
import smtplib
from email.message import EmailMessage
from os.path import basename
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

class SendMail():
    
    def Send(self, text, Subject):
        msg = EmailMessage()
        msg.set_content(text)
        msg['Subject'] = Subject
        msg['From'] = 'alarmas-aws@vcmedios.com.co'
        msg['To'] = ['mgarcia@vcmedios.com.co']
        conexion = smtplib.SMTP(host='10.10.122.17', port=25)
        conexion.ehlo()
        conexion.send_message(msg)
        conexion.quit()
        
        return

    def Send_Attach(self, text, flag):
        msg=MIMEMultipart()
        msg['From']= 'alarmas-aws@vcmedios.com.co'
        msg['To']='ingenieriavod@vcmedios.com.co'
        msg['Subject']='Resumen Upload Toolbox'
        body=MIMEText(text, 'plain')
        msg.attach(body)
        if flag:
            file=open('log.txt', 'r')
            attachment=MIMEApplication(file.read(), Name=basename('log.txt'))
            attachment['Content-Disposition']='attachment; filename="{}"'.format(basename('log.txt'))
            msg.attach(attachment)
        conexion = smtplib.SMTP(host='10.10.122.17', port=25)
        conexion.ehlo()
        conexion.send_message(msg)
        conexion.quit()
        return

#SendMail().Send("Mail test", "mail")