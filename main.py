from jinja2 import Environment, FileSystemLoader, select_autoescape, Template
import csv


# Function read csv
# file_path - path to csv; delimeter comma
def read_csv_to_named_dict(file_path):
    data = []
    try:
        with open(file_path, "r", newline="") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                data.append(row)
    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
    except Exception as e:
        print(f"An error occurred: {e}")
    return data


# Function render jinja2 template
# topics - csv file
# cluster - kafka cluster name: e.g. test-kafka
def render_json(topics, cluster):
    env = Environment(
        loader=FileSystemLoader("templates"), autoescape=select_autoescape()
    )

    template = env.get_template("rbac_template.json")
    return template.render(topics=topics, cluster_name=cluster)


# region vars
topics = read_csv_to_named_dict("./data/rbac_data.csv")
# render jinja template
result = render_json(topics, "bksblps-kafka")

# Write result to file
with open("result_rbac.json", "w+") as f:
    f.write(result)
