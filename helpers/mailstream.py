import io
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase


def export_csv(df):
    with io.StringIO() as buffer:
        df.to_csv(buffer, index=False)
        return buffer.getvalue()


async def sendmail(dataaddress, attachment_list, file_list):
    body_text = """
        Hi,
        Please find attachments for provided route plan.
        Thanks and Regards,
        Analytics Team (BE)
    """
    msg = MIMEMultipart()
    sender = 'devops@waycool.in'
    boss = 'amar.patil@waycool.in'
    msg['Subject'] = 'Route Plan'
    if sender is not None:
        msg['From'] = sender
    msg['To'] = dataaddress
    for each_frame, each_file in zip(attachment_list, file_list):
        try:
            part = MIMEBase('application', "octet-stream")
            part.set_payload(export_csv(each_frame))
            part.add_header(
                'Content-Disposition',
                'attachment',
                filename=each_file
            )
            msg.attach(part)
        except Exception as e:
            print(e)
    msg.attach(MIMEText(body_text))
    s = smtplib.SMTP('smtp.gmail.com:587')
    s.starttls()
    s.login("devops@waycool.in", "KolkataKubernetes")
    s.sendmail(
        sender,
        [dataaddress, boss],
        msg.as_string().encode('utf8')
    )
    s.quit()
