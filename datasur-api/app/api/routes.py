import json

from flask import make_response, render_template, request
from flask_restful import Resource

from app.core.engine import TradeEngine


class EmpresaResource(Resource):
    def __init__(self) -> None:
        self.engine = TradeEngine()

    def get(self, pais: str, operacion: str, categoria: str, filtro: str):
        report = self.engine.get_report(pais, operacion, categoria, filtro)

        accept = request.headers.get("Accept", "")
        if "application/json" in accept:
            return report, 200

        response = make_response(
            render_template("empresa.html", report=report, report_json=json.dumps(report))
        )
        response.headers["Content-Type"] = "text/html; charset=utf-8"
        return response
