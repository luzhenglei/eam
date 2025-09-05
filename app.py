from flask import Flask
from config import Config
from db import get_conn
from blueprints.home import home
from blueprints.options import bp_options
from blueprints.templates import bp_templates
from blueprints.attributes import bp_attrs
from blueprints.templates_admin import bp_templates_admin
from blueprints.devices import bp_devices
from blueprints.port_template import tpl_bp
from blueprints.port_types import bp_port_types
from blueprints.connect import bp_connect
from blueprints.projects import bp_projects
from blueprints.cables import bp_cables
from blueprints.connect import bp_connect


def create_app():
    app = Flask(__name__)
    app.config["SECRET_KEY"] = Config.FLASK_SECRET
    app.register_blueprint(home)
    app.register_blueprint(bp_options)
    app.register_blueprint(bp_templates)
    app.register_blueprint(bp_attrs)
    app.register_blueprint(bp_templates_admin)
    app.register_blueprint(bp_devices)
    app.register_blueprint(tpl_bp)
    app.register_blueprint(bp_port_types)
    app.register_blueprint(bp_connect)
    app.register_blueprint(bp_projects)
    app.register_blueprint(bp_cables)
    return app

app = create_app()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
