from flask import Flask


app = Flask(__name__)

from app import controller, models

if __name__ == '__main__':
	app.run()
