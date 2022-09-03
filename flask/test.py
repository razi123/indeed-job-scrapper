from flask import Flask, request, render_template, redirect, url_for
import os

test_app = Flask(__name__)

picsPath = os.path.join("static", "images")

print(picsPath)
test_app.config['UPLOAD_FOLDER'] = picsPath


@test_app.route("/")
def index():
    #pic1 = os.path.join(test_app.config['UPLOAD_FOLDER'], 'job_image.jpeg')
    #return render_template("test.html", user_image=pic1)
	return render_template("home.html")


@test_app.route("/signup")
def signup():
	return render_template("signup.html")


@test_app.route("/login")
def login():
	return render_template("login.html")


@test_app.route("/welcome/<name>")
def welcome(name):
	return "<h1> Welcome {}</h1>".format(name)


if __name__ == "__main__":
	test_app.run(host='0.0.0.0', port=5000, debug=True)