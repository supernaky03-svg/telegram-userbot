from threading import Thread

from flask import Flask

from ..config import PORT

app = Flask(__name__)


@app.route("/")
def home():
    return "Bot is alive"


@app.route("/healthz")
def healthz():
    return "ok"


def run_web() -> None:
    app.run(host="0.0.0.0", port=PORT)


def keep_alive() -> None:
    Thread(target=run_web, daemon=True).start()
