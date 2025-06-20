from flask import render_template

def register_nord_routes(app):
    @app.route("/management_report_nord")
    def management_report_nord():
        return render_template("management_report_nord.html")

    @app.route("/report1_nord")
    def report1_nord():
        return render_template("report1_nord.html")

    @app.route("/report2_nord")
    def report2_nord():
        return render_template("report2_nord.html")

    @app.route("/report3_nord")
    def report3_nord():
        return render_template("report3_nord.html")

    @app.route("/report4_nord")
    def report4_nord():
        return render_template("report4_nord.html")

    @app.route("/report6_nord")
    def report6_nord():
        return render_template("report6_nord.html")

    @app.route("/report7_nord")
    def report7_nord():
        return render_template("report7_nord.html")
