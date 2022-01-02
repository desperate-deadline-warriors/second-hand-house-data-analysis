from flask import Flask, render_template

app = Flask(__name__)

@app.route('/01')
def echart01():
    return render_template('./result.html')

if __name__ == '__main__':
    app.run()