 
from flask import Flask, jsonify
from routes.dbController import db_route

app = Flask(__name__)

app.register_blueprint(db_route)

@app.route("/")
def docs():
    return "https://github.com/levindoneto/technical-assistance-system"

if __name__ == "__main__":
    from waitress import serve
    serve(app, host="0.0.0.0", port=8010)