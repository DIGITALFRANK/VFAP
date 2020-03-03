import datetime
import os
import sys
import logging
import smtplib
import email.utils
import socket
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from modules.config import ses_config
from pyspark.sql import DataFrame


def _send_email_smtp(
    sender_address,
    recipient,
    subject,
    sender_name,
    attachment,
    body_text,
    body_html,
    smtp_username,
    smtp_password,
    smtp_host,
    smtp_port,
    log,
):
    """
    Parameters:

    sender_address: str
    recipient: str
    subject: str
    sender_name: str
    attachment: Union[str, None]
    body_text: Union[str, None]
    body_html: Union[str, None]
    smtp_username: str
    smtp_password: str
    smtp_host: str
    smtp_port: str
    log: logging.Logger

    Returns:

    True if success - raises Exception otherwise

    This function uses the Python SMTP library to construct and send an email using user specified header information
    and user specified body information. The server is contacted via SMTP using the user supplied smtp_username,
    smtp_password, smtp_host and smtp_port. The email header is formed using the sender_name, sender_address, recipient,
    subject, attachment, body_text and body_html. attachment, body_text and/or body_html can be optionally set to None if
    not applicable.
    """
    log.info("Constructing email to send via SMTP/AWS SES")
    # Create message container - the correct MIME type is multipart/alternative.
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    log.info("Email subject: {0}".format(subject))
    msg["From"] = email.utils.formataddr((sender_name, sender_address))
    log.info("Email sender: {0} - {1}".format(sender_name, sender_address))
    msg["To"] = recipient
    log.info("Email recipient: {0}".format(recipient))
    # Comment or delete the next line if you are not using a configuration set
    # msg.add_header('X-SES-CONFIGURATION-SET',CONFIGURATION_SET)

    # Add body to email
    if body_text:
        log.info("Adding plaintext body to email: {0}".format(body_text))
        textpart = MIMEText(body_text, "plain")
        msg.attach(textpart)
    if body_html:
        log.info("Adding HTML body to email: {0}".format(body_html))
        htmlpart = MIMEText(body_html, "html")
        msg.attach(htmlpart)

    # Define the attachment part and encode it using MIMEApplication.
    if attachment:
        log.info("Adding attachment to email: {0}".format(attachment))
        try:
            att = MIMEApplication(open(attachment, "rb").read())
        except FileNotFoundError as e:
            err_msg = "Unable to attach file {0} to email, file not found: {1}".format(
                attachment, e
            )
            log.error(err_msg)
            raise FileNotFoundError(err_msg)
        att.add_header(
            "Content-Disposition", "attachment", filename=os.path.basename(attachment)
        )
        msg.attach(att)
    # Try to send the message.
    log.info("Attempting to send email")
    try:
        server = smtplib.SMTP(smtp_host, smtp_port, timeout=60)
    except socket.timeout as e:
        err_msg = "SMTP server timed out - check that the following parameters are correct:\n\tSMTP host - {0}\n\tSMTP port - {1}".format(
            smtp_host, smtp_port
        )
        log.error(err_msg)
        raise Exception(err_msg)
    log.info("Established connection with SMTP server")

    # stmplib docs recommend calling ehlo() before & after starttls()
    try:
        server.ehlo()
        server.starttls()
        server.ehlo()
    except BaseException as e:
        err_msg = "Unable to establish TLS connectivity with remote SMTP server: {0}".format(
            e
        )
        log.error(err_msg)
        raise Exception(err_msg)

    log.info("Logging into SMTP server with provided username and password")
    try:
        server.login(smtp_username, smtp_password)
        log.info("Successfully logged in to SMTP server")
    except BaseException as e:
        err_msg = "Unable to login to SMTP server - check that provided username and password are correct: {0}".format(
            e
        )
        log.error(err_msg)
        raise Exception(err_msg)

    log.info("Sending email...")
    try:
        # sendmail method returns a dict with a key-value pair for each failed send
        # returns empty dict if successful
        send_status = server.sendmail(sender_address, recipient, msg.as_string())
        if len(send_status) >= 1:
            raise Exception("{0}".format(str(send_status)))
    except Exception as e:
        err_msg = "Sending failed for one or more recipients: {0}".format(e)
        log.error(err_msg)
        raise Exception(err_msg)
    server.close()
    log.info("Email sent successfully")
    return True


def send_job_status_email(
    job_status,
    start_time,
    end_time,
    file_name,
    log,
    exception=None,
    environment=ses_config.environment,
    sender_address=ses_config.job_status_email_sender_address,
    recipient_list=ses_config.job_status_email_recipient_address_list,
    sender_name=ses_config.job_status_email_sender_name,
    smtp_username=ses_config.smtp_username,
    smtp_password=ses_config.smtp_password,
    smtp_host=ses_config.smtp_host,
    smtp_port=ses_config.smtp_port,
):
    """
    Parameters:

    job_status: str
    start_time: str
    end_time: str
    file_name: str
    log: logging.Logger
    exception: Union[None, Exception]
    environment: str
    sender_address: str
    recipient_list: List[str]
    sender_name: str
    smtp_username: str
    smtp_password: str
    smtp_host: str
    smtp_port: str


    Returns

    True if success - raises Exception otherwise
    """
    log.info("Building job completion email notification")
    log.debug("Validating user input")
    if type(job_status) != str:
        log.error(
            "Expected status parameter to be string type, got {0}".format(
                type(job_status)
            )
        )
        raise TypeError
    if type(environment) != str:
        log.error(
            "expected environment parameter to be string type, got {0}".format(
                type(environment)
            )
        )
        raise TypeError
    if type(file_name) != str:
        log.error(
            "expected file_name parameter to be string type, got {0}".format(
                type(file_name)
            )
        )
        raise TypeError
    if type(start_time) != str:
        log.error(
            "expected start_time parameter to be string type, got {0}".format(
                type(start_time)
            )
        )
        raise TypeError
    if type(end_time) != str:
        log.error(
            "expected end_time parameter to be string type, got {0}".format(
                type(end_time)
            )
        )
        raise TypeError

    # Construct subject
    subject = (
        environment
        + " : "
        + file_name
        + " - "
        + job_status
        + ". "
        + start_time
        + " - "
        + end_time
    )

    # Construct body
    if exception is not None:
        body_text = "Job Failed - {0}".format(exception)
    else:
        body_text = "Job Successful!"
    for recipient in recipient_list:

        _send_email_smtp(
            sender_address=sender_address,
            recipient=recipient,
            subject=subject,
            sender_name=sender_name,
            attachment=None,
            body_text=body_text,
            body_html=None,
            smtp_username=smtp_username,
            smtp_password=smtp_password,
            smtp_host=smtp_host,
            smtp_port=smtp_port,
            log=log,
        )
    return True


def _create_html_report_string_head(log):
    """
    Parameters:

    log: logging.Logger

    Returns:

    str - Beginning of HTML report

    This function produces summary HTML output which displays the content of a summary sized table in PySpark
    """
    # Build a templated HTML string defining a table with titles
    html_string = """
<!DOCTYPE html>
<html>
<head>
<style>
table, th, td {
    border: 1px solid black;
    border-collapse: collapse;
}
th, td {
  padding: 5px;
}
th {
  text-align: left;
}
</style>
</head>
<body>"""
    return html_string


def _append_spark_table_to_html_string(html_string, title, df, footnote, log):
    """
    Parameters:

    html_string: str
    title: str
    df: pyspark.sql.DataFrame
    footnote: str
    log: logging.Logger

    Returns:

    str - html_string extended to include html formatted table

    This function appends the HTML for a small table from PySpark and finishes the message if a footnote is some string which is not None
    """
    # Build an HTML string specifying line and 4 spaces delimited table headers
    cols = df.columns
    col_string = "\n    ".join(["<th>{0}</th>".format(col) for col in cols])
    html_string += """
<p></p>
<h4>{0}</h4>
<table style="width:50%">
  <tr>
    {1}
  </tr>""".format(
        title, col_string
    )
    # Get the content of a table from the SparkSQL catalogue
    # WARNING: The table must be small enough to collect to the Driver program
    # Iterate through the row objects, adding a new tr HTML element for each row
    for row in df.collect():
        html_string += "\n  <tr>"
        for col in cols:
            # Iterate through each field of a given row, adding a new td HTML element for each field
            html_string += "\n    <td>{0}</td>".format(row[col])
        html_string += "\n  </tr>"
    # Close the table element in the HTML
    html_string += "\n</table>"
    # Add a footnote to the HTML if specified
    if footnote != None:
        html_string += "\n<p>\n  <text>{0}</text>\n</p>\n</body>\n</html>".format(
            footnote
        )
    return html_string


def build_html_summary(dataframes, table_titles, footnote, log):
    """
    Parameters:

    table_titles: List[str]
    dataframes: List[str]
    footnote: List[str]
    log: logging.Logger

    Returns:

    str or None

    This function builds an html summary of one or more Spark DataFrames
    """
    log.info("Creating HTML report")
    head = _create_html_report_string_head(log)
    html_string = head
    max_index = len(dataframes)
    for index in range(len(dataframes)):
        if index == max_index - 1:
            return _append_spark_table_to_html_string(
                html_string=html_string,
                title=table_titles[index],
                df=dataframes[index],
                footnote=footnote,
                log=log,
            )
        else:
            html_string = _append_spark_table_to_html_string(
                html_string=html_string,
                title=table_titles[index],
                df=dataframes[index],
                footnote=None,
                log=log,
            )


def validate_non_empty_string(value, field_name, log):
    """
    Parameters:

    value: Any
    field_name: str
    log: logging.Logger

    Returns:

    None if success, raises ValueError exception if validation fails

    This function validates whether a value is a non-empty string or not. Raises a
    ValueError exception if not.
    """
    try:
        assert type(value) is str
        assert len(value) >= 1
    except AssertionError as e:
        log.error(
            "{0} should be a string of at least one character long - {1}".format(
                field_name, str(value)
            )
        )
        raise Exception(
            "{0} should be a string of at least one character long - {1}".format(
                field_name, str(value)
            )
        )
    return None


def validate_list(input_list, element_type, list_name, log):
    """
    Parameters:

    input_list: List[element_type]
    element_type: [Any]
    list_name: str
    log: logging.Logger

    Returns:

    None or raises Exception if validation fails

    This function tests the following criteria:

        * input_list is of type list
        * input_list contains types of only element_type

    """
    try:
        assert type(input_list) is list
    except AssertionError:
        log.error(
            "Encountered unexpected data type for {0} - {1} - should be a list of {2}".format(
                list_name, type(input_list), element_type
            )
        )
        raise TypeError(
            "Encountered unexpected data type for {0} - {1} - should be a list of {2}".format(
                list_name, type(input_list), element_type
            )
        )
    try:
        for element in input_list:
            assert type(element) is element_type
    except AssertionError:
        log.error(
            "Encountered non-{0} element in {1} - {2}".format(
                element_type, list_name, str(input_list)
            )
        )
        raise TypeError(
            "Encountered non-{0} element in {1} - {2}".format(
                element_type, list_name, str(input_list)
            )
        )

    return


def send_report_email(
    job_name,
    subject,
    dataframes,
    table_titles,
    log,
    environment=ses_config.environment,
    sender_address=ses_config.reporting_email_sender_address,
    recipient_list=ses_config.reporting_email_recipient_address_list,
    sender_name=ses_config.reporting_email_sender_name,
    smtp_username=ses_config.smtp_username,
    smtp_password=ses_config.smtp_password,
    smtp_host=ses_config.smtp_host,
    smtp_port=ses_config.smtp_port,
):
    """
    Parameters:

    job_name: str
    subject: str
    dataframes: List[pyspark.sql.DataFrame]
    table_titles: List[str]
    log: logging.Logger
    environment: str
    sender_address: str
    recipient_list: List[str]
    sender_name: str
    smtp_username: str
    smtp_password: str
    smtp_host: str
    smtp_port: str

    Return:

    None if successful, raises Exception otherwise

    Sends email using SES to list of recipients - generates HTML report for list of DataFrames if included
    in parameters with corresponding list of titles for tables
    """
    log.info("Initializing email notification procedure for {0}".format(job_name))

    validate_list(recipient_list, str, "recipients", log)
    validate_non_empty_string(subject, "subject", log)

    log.info("Constructing email(s) with HTML report")
    validate_list(dataframes, DataFrame, "dataframes", log)
    validate_list(table_titles, str, "table_titles", log)
    try:
        assert len(dataframes) == len(table_titles)
    except AssertionError:
        log.error(
            "Encountered {0} table titles and {1} DataFrames for HTML report - match required".format(
                len(table_titles), len(dataframes)
            )
        )
        raise ValueError(
            "Encountered {0} table titles and {1} DataFrames for HTML report - match required".format(
                len(table_titles), len(dataframes)
            )
        )

    html = build_html_summary(
        dataframes=dataframes,
        table_titles=table_titles,
        footnote=str(datetime.datetime.utcnow()),
        log=log,
    )

    for recipient in recipient_list:

        _send_email_smtp(
            sender_address=sender_address,
            recipient=recipient,
            subject=subject,
            sender_name=sender_name,
            attachment=None,
            body_text=None,
            body_html=html,
            smtp_username=smtp_username,
            smtp_password=smtp_password,
            smtp_host=smtp_host,
            smtp_port=smtp_port,
            log=log,
        )

    return

def send_absent_report_email(
    job_name,
    subject,
    message,
    log,
    environment=ses_config.environment,
    sender_address=ses_config.reporting_email_sender_address,
    recipient_list=ses_config.reporting_email_recipient_address_list,
    sender_name=ses_config.reporting_email_sender_name,
    smtp_username=ses_config.smtp_username,
    smtp_password=ses_config.smtp_password,
    smtp_host=ses_config.smtp_host,
    smtp_port=ses_config.smtp_port,
):
    """
    Parameters:

    job_name: str
    subject: str
    message: str
    log: logging.Logger
    environment: str
    sender_address: str
    recipient_list: List[str]
    sender_name: str
    smtp_username: str
    smtp_password: str
    smtp_host: str
    smtp_port: str

    Return:

    None if successful, raises Exception otherwise

    Sends email using SES to list of recipients - generates HTML report for list of DataFrames if included
    in parameters with corresponding list of titles for tables
    """
    log.info("Initializing email notification procedure for {0}".format(job_name))

    for recipient in recipient_list:

        _send_email_smtp(
            sender_address=sender_address,
            recipient=recipient,
            subject=subject,
            sender_name=sender_name,
            attachment=None,
            body_text=message,
            body_html=None,
            smtp_username=smtp_username,
            smtp_password=smtp_password,
            smtp_host=smtp_host,
            smtp_port=smtp_port,
            log=log,
        )

    return
