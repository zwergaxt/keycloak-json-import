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

def get_describe_config(data):
    describe_data = []
    resources = []
    for i in data:
        resources.append(i.get("resource_name"))
    
    resources = list(set(resources))
    for i in resources:
        tmp_data = []
        
        for d in data:
            if d.get("resource_name") == i:
                tmp_data.append(d.get("ad_group_name"))

        describe_data.append({'ad_group_name': tmp_data, 'resource_name': i})
    return describe_data

topics = read_csv_to_named_dict("./data/rbac_data_example.csv")

# Function render jinja2 template
# topics - csv file
# cluster - kafka cluster name: e.g. test-kafka
def render_json(topics, cluster, describe_rules):
    env = Environment(
        loader=FileSystemLoader("templates"), autoescape=select_autoescape()
    )

    template = env.get_template("rbac_template.json")
    return template.render(topics=topics, cluster_name=cluster, describe_rules=describe_rules)


# region vars
topics = read_csv_to_named_dict("./data/rbac_data_example.csv")
# render jinja template
result = render_json(topics, "bksblps-kafka", describe_rules=get_describe_config(topics))

# Write result to file
with open("result_rbac.json", "w+") as f:
    f.write(result)
