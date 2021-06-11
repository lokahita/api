from flask import Flask
from flask_mail import Mail, Message

app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret'
app.config['DEBUG'] = True

app.config["MAIL_SERVER"] = "webmail.big.go.id"
app.config["MAIL_PORT"] = 465
app.config["MAIL_USE_SSL"] = True
app.config["MAIL_USERNAME"] = 'application.support@big.go.id'
app.config["MAIL_PASSWORD"] = '@ppsb1g'
app.config["MAIL_DEFAULT_SENDER"] = 'application.support@big.go.id'

mail = Mail(app)

@app.route("/")
def index():
    return "send email /kirim_email"

@app.route("/kirim_email")
def kirim_email():
    print(mail.connect())
    subject = "[InaGeoportal] Info Pendaftaran"
    msg = Message(subject, recipients=["muhammad.hasannudin@big.go.id"])
    #msg.body = "testing"
    msg.html = "<b>testing</b>"
    with app.app_context():
        mail.send(msg)
        return "OK"


#
#with mail.connect() as conn:
#    for user in users:
#        message = '...'
#        subject = "hello, %s" % user.name
#        msg = Message(recipients=[user.email],
#                      body=message,
#                      subject=subject)
#        conn.send(msg)
#
if __name__ == '__main__':
    app.run()