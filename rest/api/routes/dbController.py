from flask import Blueprint, request, jsonify
from services import db_services as Db

db_route = Blueprint('db_route', __name__)

@db_route.route("/reports/categories", methods=["GET"])
def getCategoriesReport():
    return jsonify(Db.getCategoriesReport())

@db_route.route("/reports/providersnotbought", methods=["GET"])
def getProvidersNotBoughtReport():
    return jsonify(Db.getProvidersNotBoughtReport())

@db_route.route("/reports/clientswhobought", methods=["GET"])
def getClientsWhoBoughtReport():
    return jsonify(Db.getClientsWhoBoughtReport())

@db_route.route("/reports/ostotal", methods=["GET"])
def getOSTotalReport():
    return jsonify(Db.getOSTotalReport())

@db_route.route("/reports/billstopay", methods=["GET"])
def getBillsToPay():
    return jsonify(Db.getBillsToPay())

@db_route.route("/reports/billstoreceive", methods=["GET"])
def getBillsToReceive():
    return jsonify(Db.getBillsToReceive())

@db_route.route("/reports/registertransactions", methods=["GET"])
def getRegisterTransactions():
    idRegister = request.args.get('idRegister')
    return jsonify(Db.getRegisterTransactions(idRegister))

@db_route.route("/allregisters", methods=["GET"])
def getRegisters():
    return jsonify(Db.getRegisters())

@db_route.route("/allproducts", methods=["GET"])
def getProducts():
    return jsonify(Db.getProducts())

@db_route.route("/categories", methods=["GET"])
def getCategoriesByProductId():
    productId = request.args.get('productId')
    return jsonify(Db.getCategoriesByProductId(productId))

@db_route.route("/suppliers", methods=["GET"])
def getSuppliersByProductId():
    productId = request.args.get('productId')
    return jsonify(Db.getSuppliersByProductId(productId))