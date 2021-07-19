from flask import Flask, render_template
from flask_bootstrap import Bootstrap
import pandas as pd
#循环引用，解决方法，推迟一方的导入，让例外一方完成


app = Flask(__name__)
bootstrap = Bootstrap(app)


def read_data(bank):
    return pd.read_excel("pvgo.xlsx", sheet_name=bank, engine='openpyxl')


@app.route('/')
def index():
    return render_template(
        "base.html")

@app.route('/zb')
def get_stock_zb():
    df = read_data("主板")
    return render_template(
        "stock.html", data=df.to_html(classes="table table-striped", index=False)
    )

@app.route('/zxb')
def get_stock_zxb():
    df = read_data("中小板")
    return render_template(
        "stock.html", data=df.to_html(classes="table table-striped", index=False)
    )

@app.route('/cyb')
def get_stock_cyb():
    df = read_data("创业板")
    return render_template(
        "stock.html", data=df.to_html(classes="table table-striped", index=False)
    )

@app.route('/kcb')
def get_stock_kcb():
    df = read_data("科创板")
    return render_template(
        "stock.html", data=df.to_html(classes="table table-striped", index=False)
    )

@app.errorhandler(404)
def page_not_found(e):
    return render_template(
        "404.html"), 404

@app.errorhandler(500)
def internal_server_error(e):
    return render_template(
        "500.html"), 500

if __name__ == '__main__':
    print(app.url_map)
    app.run(host='0.0.0.0', port=5000, debug=True)
