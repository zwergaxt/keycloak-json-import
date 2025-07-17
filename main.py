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

# Function delete dups
def remove_duplicates(data_list):
    return list(set(data_list))

# Function create data struct for common permissions
def get_describe_config(data):
    describe_data = []

    # get list of resource (topics) names
    resources = []
    for i in data:
        resources.append(i.get("resource_name"))

    resources = remove_duplicates(resources)

    for i in resources:
        tmp_data = []

        for d in data:
            if d.get("resource_name") == i:
                tmp_data.append(d.get("ad_group_name"))

        describe_data.append({"ad_group_name": tmp_data, "resource_name": i})

    return describe_data

# Get groups list and clean it
def get_groups(data):
    groups_data = []

    for i in data:
        groups_data.append(i.get("ad_group_name"))

    groups_data = remove_duplicates(groups_data)

    return groups_data

# Function render jinja2 template
# topics - csv file
# cluster - kafka cluster name: e.g. test-kafka
# describe_rules - set of topic and groups for common permissions
def render_json(data, cluster, describe_rules, groups_data):
    env = Environment(
        loader=FileSystemLoader("templates"), autoescape=select_autoescape()
    )

    template = env.get_template("rbac_template.json.j2")
    return template.render(
        data=data, cluster_name=cluster, describe_rules=describe_rules, groups_data=groups_data
    )


data_path = "./data/rbac_data_example.csv"
data = read_csv_to_named_dict(data_path)
# render jinja template
result = render_json(
    data=data, cluster="bksblps-kafka", describe_rules=get_describe_config(data), groups_data=get_groups(data)
)

# Write result to file
with open("result_rbac.json", "w+") as f:
    f.write(result)
