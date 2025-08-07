import typer
# from ETL.defillama.main import app as defillama_app
from ETL.binance.main import app as binance_app

app = typer.Typer()
# app.add_typer(defillama_app, name='defillama')
app.add_typer(binance_app, name='binance')

if __name__ == "__main__":
    app()