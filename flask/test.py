from flask import Flask, request, render_template, redirect, url_for
import os

test_app = Flask(__name__)

picsPath = os.path.join("static", "images")

print(picsPath)
test_app.config['UPLOAD_FOLDER'] = picsPath


@test_app.route("/")
def index():
	return render_template("base.html")


@test_app.route("/signup")
def signup():
	return render_template("signup.html")

@test_app.route("/thankyou")
def thankyou():
	username = request.args.get("username")
	email = request.args.get("email")
	password = request.args.get("pword")
	return render_template("thankyou.html", name= username)


@test_app.route("/login")
def login():
	return render_template("login.html")


@test_app.route("/subscribe_jobs")
def subscribe_jobs():
	return render_template("subscribe_jobs.html")


if __name__ == "__main__":
	test_app.run(host='0.0.0.0', port=6000, debug=True)