##########################################################
# Import
from email.utils import COMMASPACE
import boto3
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import os


##########################################################
# class

class SESMessage(object):
    """
    Usage:

    msg = SESMessage('from@example.com', 'to@example.com', 'The subject')
    msg.text = 'Text body'
    msg.html = 'HTML body'
    msg.send()

    """

    def __init__(self, source, to_addresses, subject, **kw):
        self.ses = boto3.client('ses', region_name='us-east-1')

        self._source = source
        self._to_addresses = to_addresses
        self._cc_addresses = []
        self._bcc_addresses = []

        self.subject = subject
        self.text = None
        self.html = None
        self.attachment = None

    def send(self):
        """
        Actual function to send email.
        :return:
        """

        # check if ses object is present if not raise exception.
        if not self.ses:
            raise Exception('No SES connection found')

        # check if only either text or html is present
        if (self.text and not self.html and not self.attachment) or \
                (self.html and not self.text and not self.attachment):
            return self.ses.send_email(Source=self._source,
                                       Destination={
                                           'ToAddresses': self._to_addresses,
                                           'CcAddresses': self._cc_addresses,
                                           'BccAddresses': self._bcc_addresses
                                       },
                                       Message={
                                           'Body': {
                                               'Html': {
                                                   'Charset': 'utf-8',
                                                   'Data': self.html if self.html is not None else '<p></p>',
                                               },
                                               'Text': {
                                                   'Charset': 'utf-8',
                                                   'Data': self.text if self.text is not None else '',
                                               },
                                           },
                                           'Subject': {
                                               'Charset': 'utf-8',
                                               'Data': self.subject,
                                           },
                                       })
        else:

            # case when both plain and html part is present in the email message.
            if not self.attachment:
                message = MIMEMultipart('alternative')

                message['Subject'] = self.subject
                message['From'] = self._source
                if isinstance(self._to_addresses, (list, tuple)):
                    message['To'] = COMMASPACE.join(self._to_addresses)
                else:
                    message['To'] = self._to_addresses

                message.attach(MIMEText(self.text, 'plain'))
                message.attach(MIMEText(self.html, 'html'))

            # case when there are attachments
            else:

                message = MIMEMultipart('mixed')
                message['Subject'] = self.subject
                message['From'] = self._source
                if isinstance(self._to_addresses, (list, tuple)):
                    message['To'] = COMMASPACE.join(self._to_addresses)
                else:
                    message['To'] = self._to_addresses

                # Create a multipart/alternative child container.
                message_alt = MIMEMultipart('alternative')

                # Add the text and HTML parts to the child container.
                if self.text:
                    message_alt.attach(MIMEText(self.text, 'plain', 'UTF-8'))
                if self.html:
                    message_alt.attach(MIMEText(self.html, 'html', 'UTF-8'))

                # Attach the multipart/alternative child container to the multipart/mixed
                # parent container.
                message.attach(message_alt)

                # Define the attachment part and encode it using MIMEApplication.
                att = MIMEApplication(open(self.attachment, 'rb').read())

                # Add a header to tell the email client to treat this part as an attachment,
                # and to give the attachment a name.
                att.add_header('Content-Disposition', 'attachment', filename=os.path.basename(self.attachment))

                # Add the attachment to the parent container.
                message.attach(att)

            return self.ses.send_raw_email(
                Source=self._source,
                RawMessage=message.as_string(),
                destinations=self._to_addresses)
