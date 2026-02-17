from app import create_app


def test_json_response(monkeypatch):
    app = create_app()
    client = app.test_client()

    monkeypatch.setattr(
        "app.api.routes.TradeEngine.get_report",
        lambda *_args, **_kwargs: {
            "meta": {},
            "total_fob": 10.0,
            "partner_ranking": [],
            "transport_ranking": [],
            "monthly_evolution": [],
        },
    )

    resp = client.get(
        "/paises/chile/exportaciones/empresa/acme/",
        headers={"Accept": "application/json"},
    )

    assert resp.status_code == 200
    assert resp.get_json()["total_fob"] == 10.0
