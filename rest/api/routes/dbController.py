from flask import Blueprint, request, jsonify
from services import db_services as Db

db_route = Blueprint('db_route', __name__)

@db_route.route("/categories", methods=["GET"])
def getCategoriesReport():
    return jsonify(Db.getCategoriesReport())