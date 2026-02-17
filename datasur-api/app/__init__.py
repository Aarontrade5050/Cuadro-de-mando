from flask import Flask
from flask_restful import Api

from app.api.routes import EmpresaResource


def create_app() -> Flask:
    """Application factory for Datasur API."""
    app = Flask(__name__)
    api = Api(app)

    api.add_resource(
        EmpresaResource,
        "/paises/<string:pais>/<string:operacion>/<string:categoria>/<string:filtro>/",
    )

    return app
