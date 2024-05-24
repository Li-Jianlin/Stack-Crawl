import zmail


def send_email(my_subject, my_content):
    msg = {
        'subject': my_subject,
        'content_text': my_content
    }
    server = zmail.server('2285687467@qq.com', 'wzekzueiuxpndida')
    server.send_mail(['2285687467@qq.com','3145971793@qq.com'], msg)

def send_email_test(my_subject, my_content):
    msg = {
        'subject': my_subject,
        'content_text': my_content
    }
    server = zmail.server('2285687467@qq.com', 'wzekzueiuxpndida')
    server.send_mail(['2285687467@qq.com'], msg)