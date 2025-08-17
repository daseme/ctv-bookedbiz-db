import os
import re

TEMPLATE_DIR = "src/web/templates"
OUTPUT_FILE = "src/web/auto_generated_nord_routes.py"

def get_nord_templates():
    return sorted(
        f for f in os.listdir(TEMPLATE_DIR)
        if f.endswith("_nord.html")
    )

def to_route_name(filename):
    return filename.replace(".html", "")

def to_endpoint_name(filename):
    return filename.replace(".html", "").replace("-", "_")

def generate_route_stub(route):
    endpoint = to_endpoint_name(route)
    return f"""@app.route("/{route}")
def {endpoint}():
    return render_template("{route}.html")\n"""

def main():
    templates = get_nord_templates()

    with open(OUTPUT_FILE, "w") as f:
        f.write("from flask import render_template\nfrom .app import app\n\n")
        for tmpl in templates:
            route = to_route_name(tmpl)
            f.write(generate_route_stub(route))

    print(f"✅ Routes written to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
