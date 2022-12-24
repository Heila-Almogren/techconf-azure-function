import logging
import azure.functions as func
import psycopg2
import os
from datetime import datetime
from sqlalchemy import create_engine, orm
from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy as sa
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

def main(msg: func.ServiceBusMessage):

    notification_id = int(msg.get_body().decode('utf-8'))
    logging.info(notification_id)
    # TODO: Get connection to database
    conn = psycopg2.connect(os.environ["myDbConnectionString"])

    try:
        print("hello")
        cur = conn.cursor()

        # TODO: Get notification message and subject from database using the notification_id
        cur.execute("SELECT subject, message FROM notification where id = "+str(notification_id))
        notification_info = cur.fetchone()
        subject = notification_info[0]
        message = notification_info[1]

        # TODO: Get attendees email and name
        cur.execute("SELECT first_name, email from attendee")
        rows = cur.fetchall()
        
        # TODO: Loop through each attendee and send an email with a personalized subject
        for row in rows:
            email_subject = '{}: {}'.format(row[0], subject)
            send_email(row[1], email_subject, message)

        # TODO: Update the notification table by setting the completed date and updating the status with the total number of attendees notified
        
        count = cur.rowcount
        date = datetime.utcnow()
        cur.execute("UPDATE notification set completed_date = '" + str(date) + "', status = 'Notified " + str(count) + " attendees' where id = " + str(notification_id))
        conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
    finally:
        conn.close()


def send_email(email, subject, body):
    if os.environ["mysendGridKey"]:
        message = Mail(
            from_email=os.environ['ADMIN_EMAIL_ADDRESS'],
            to_emails=email,
            subject=subject,
            plain_text_content=body)

        sg = SendGridAPIClient(os.environ["mysendGridKey"])
        sg.send(message)
