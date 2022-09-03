from flask import Flask, request, render_template, redirect, url_for

app = Flask(__name__)


@app.route("/")
def home():
	return render_template("login.html")

database= {"razi" : "123"}


@app.route('/form_login', methods=['POST','GET'])
def login():
	name1 = request.form['username']
	pwd = request.form['password']
	if name1 in database:
		if database[name1]!= pwd:
			return render_template("login.html", info="Invalid Password")
		else:
			return render_template("welcome.html", name=name1)
	else:
		return render_template('signup.html', info='Signup Please')


@app.route("/form_signup", methods=["GET", "POST"])
def signup():
	name2 = request.form["username"]
	pwd2 = request.form["password"]

	database[name2] = pwd2
	return render_template("welcome.html", name=name2)


if __name__ == "__main__":
	app.run(host='0.0.0.0', port=4000, debug=True)

